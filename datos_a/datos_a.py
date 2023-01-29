import logging
import logging.config
import re
import xml.etree.ElementTree as Et
from collections import Counter
from datetime import datetime
from functools import reduce
from multiprocessing import Pool
from pathlib import Path


def chunkify(data, number_of_chunks):
    '''
    generator that receives data and returns the portion of this
    data

    Params:
            (list): data
            (int):number_of_chunks
    Returns:
                    (int): current age

    '''
    for i in range(0, len(data), number_of_chunks):
        yield data[i:i + number_of_chunks]


def get_tags(data):
    '''
    function mapped that receives data an return tags information
        Params:
            (list): data
    Returns:
            (list): tags

    '''
    try:
        if 'AcceptedAnswerId' in data.attrib:
            tags = data.attrib.get("Tags")
            return tags
    except Exception:
        logger.error("Error al traer atributos Tags")


def mapper_tags(tags):
    '''
    function that clean tags data

    Params:
            (list): tags
    Returns:
            (list): tags

    '''
    tags = tags[1:-1].split('><')
    return tags


def reducer_tags(cnt1, cnt2):
    '''
    function that concat data tags

    Params:
            (list): cnt1
            (list): cnt1
    Returns:
            (list): cnt1

    '''
    cnt1 = cnt1 + cnt2
    return cnt1


def chunks_mapper_tags(chunk):
    '''
    function that process with MapReduce
    the data in chunks

    Returns:
            (list): reduced_chunk

    '''
    mapped_chunk = map(get_tags, chunk)
    mapped_chunk = filter(None, mapped_chunk)
    mapped_chunk = map(mapper_tags, mapped_chunk)
    reduced_chunk = reduce(reducer_tags, mapped_chunk, [])
    return reduced_chunk


def get_values(data):
    '''
    function mapped that receives data an return quantity of
    words on body and score of post
        Params:
            (list): data
    Returns:
            (dict): words,score,0

    '''
    try:
        body = data.attrib.get("Body")
        words = re.findall(
            r'(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))', body)
        score = int(data.attrib.get("Score"))
        # 0 is added for future count
        return (len(words), score, 0)
    except Exception:
        logger.error("Error al traer atributos Body")


def reducer_counter(cnt1, cnt2):
    '''
    function that accum data words and score and count
    quantity of elements

    Params:
            (list): cnt1
            (list): cnt1
    Returns:
            (list): cnt1

    '''
    (words1, score1, acc1) = cnt1
    (words2, score2, acc2) = cnt2
    cnt1 = (words1+words2, score1+score2, acc2+acc1+1)
    return cnt1


def chunks_mapper_counter(chunk):
    '''
    function that process with MapReduce
    the data in chunks

    Returns:
            (list): reduced_chunk

    '''
    mapped_chunk = map(get_values, chunk)
    return reduce(reducer_counter, mapped_chunk)


def get_coefficients(data):
    '''
    function mapped that receives data an return deltas and
    coefficients from pearson
        Params:
            (list): data
    Returns:
            (list): [coef1, coef2, coef3]

    '''
    try:
        body = data.attrib.get("Body")
        words = len(re.findall(
            r'(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))', body))
        score = int(data.attrib.get("Score"))
        delta_words = words - mean_words
        delta_score = score - mean_score
        coef1 = delta_words * delta_score
        coef2 = delta_words * delta_words
        coef3 = delta_score * delta_score
        return [coef1, coef2, coef3]
    except Exception:
        logger.error("Error al traer atributos Body")


def reducer_pearson(cnt1, cnt2):
    '''
    function that receives the coefficients and
    returns the sum of them
    Params:
            (list): cnt1,cnt2
    Returns:
            (list): cnt1

    '''
    cnt1 = [x + y for x, y in zip(cnt1, cnt2)]
    return cnt1


def chunks_mapper_pearson(chunk):
    '''
    function that process with MapReduce
    the data in chunks

    Returns:
            (list): reduced_chunk

    '''
    mapped_chunk = map(get_coefficients, chunk)
    return reduce(reducer_pearson, mapped_chunk)


def get_questions(data):
    '''
    function mapped that receives data an return id of question and
    Creation Date
        Params:
            (list): data
    Returns:
            (dict): post_id, date

    '''
    try:
        if data.attrib['PostTypeId'] == '1':
            post_id = data.attrib.get("Id")
            date = data.attrib.get("CreationDate")
            return post_id, date
    except Exception:
        logger.error("Error al traer atributos ID y/o CreationDate")


def reducer_chunk_questions(cnt1, cnt2):
    '''
    function that append data mapped

    Params:
            (list): cnt1
            (list): cnt1
    Returns:
            (list): cnt1

    '''
    cnt1.append(cnt2)
    return cnt1


def reducer_questions(cnt1, cnt2):
    '''
    function that concat data chunked

    Params:
            (list): cnt1
            (list): cnt1
    Returns:
            (list): cnt1

    '''
    cnt1 = cnt1 + cnt2
    return cnt1


def chunks_mapper_questions(chunk):
    '''
    function that process with MapReduce
    the data in chunks

    Returns:
            (list): reduced_chunk

    '''
    mapped_chunk = map(get_questions, chunk)
    mapped_chunk = filter(None, mapped_chunk)
    return reduce(reducer_chunk_questions, mapped_chunk, [])


def get_answers(data):
    '''
    function mapped that receives data an return Parent id
    of answer and CreationDate
    Params:
            (list): data
    Returns:
            (dict): parent_id, date

    '''
    try:
        if data.attrib['PostTypeId'] == '2':
            parent_id = data.attrib.get("ParentId")
            date = data.attrib.get("CreationDate")
            return parent_id, date
    except Exception:
        logger.error("Error al traer atributos ParentId y/o CreationDate")


def get_times(data):
    '''
    function mapped that receives data of answer and search for
    the question corresponding to the answer and add to the
    dictionary the creation time of said question

    Params:
            (list): data
    Returns:
            (dict): parent_id, date_answer, date_question

    '''
    for i in range(len(reduced_questions)):
        if reduced_questions[i][0] == data[0]:
            break
    return data[0], data[1], reduced_questions[i][1]


def reducer_chunk_answer_time(cnt1, cnt2):
    '''
    function that calculate the timedelta of the
    difference between the question and the answer
    in seconds

    Params:
            (list): cnt1
            (list): cnt2
    Returns:
            (list): cnt1

    '''
    (id, date_answer, date_question) = cnt2
    date_answer = datetime.fromisoformat(date_answer)
    date_question = datetime.fromisoformat(date_question)
    cnt1.append((date_answer-date_question).total_seconds())
    return cnt1


def reducer_answer_time(cnt1, cnt2):
    '''
    function that concat timedeltas on seconds

    Params:
            (list): cnt1
            (list): cnt2
    Returns:
            (list): cnt1

    '''
    cnt1 = cnt1 + cnt2
    return cnt1


def chunks_mapper_answer_time(chunk):
    '''
    function that process with MapReduce
    the data in chunks

    Returns:
            (list): reduced_chunk

    '''
    mapped_chunk = map(get_answers, chunk)
    mapped_chunk = filter(None, mapped_chunk)
    mapped_chunk = map(get_times, mapped_chunk)
    return reduce(reducer_chunk_answer_time, mapped_chunk, [])


if __name__ == '__main__':

    root_path = Path(__file__).parents[0]
    logging_config_path = Path(
            root_path,
            'datos_a/config/logging.cfg')
    dataset_path = Path(
                root_path,
                '112010 Meta Stack Overflow/posts.xml')

    logging.config.fileConfig(logging_config_path)

    logger = logging.getLogger("analyzer")

    try:
        data = Et.parse(dataset_path)
        root = data.getroot()
    except Et.ParseError:
        logger.error("error al realizar parse de archivo xml")

    pool = Pool(8)

    # top10 tags calculation
    data_chunks_tags = chunkify(root, 96)
    mapped_tags = map(chunks_mapper_tags, data_chunks_tags)
    reduced_tags = reduce(reducer_tags, mapped_tags, [])
    logger.info(" Lista del TOP 10 de tags mas utilizados"
                f"{Counter(reduced_tags).most_common(10)}")
    data_chunks_counter = chunkify(root, 96)
    mapped_counter = pool.map(chunks_mapper_counter, data_chunks_counter)
    reduced_counter = reduce(reducer_counter, mapped_counter)

    # mean calculation of words and score
    mean_words = reduced_counter[0] / reduced_counter[2]
    mean_score = reduced_counter[1] / reduced_counter[2]
    data_chunks_pearson = chunkify(root, 96)
    mapped_pearson = map(chunks_mapper_pearson, data_chunks_pearson)
    sumatories_pearson = reduce(reducer_pearson, mapped_pearson)

    # the coefficient is calculated with through the equation
    coef_pearson = sumatories_pearson[0] / (
        (sumatories_pearson[1] * sumatories_pearson[2])**0.5)

    logger.info(" Coeficiente de Pearson para verificar relacion"
                f"entre palabras de post y puntaje {coef_pearson}")

    # generate the information of questions createDate
    data_chunks_questions = chunkify(root, 96)
    mapped_pearson = pool.map(chunks_mapper_questions, data_chunks_questions)
    reduced_questions = reduce(reducer_questions, mapped_pearson, [])
    data_chunks_answer_time = chunkify(root, 96)
    mapped_answer_time = map(
        chunks_mapper_answer_time, data_chunks_answer_time)
    reduced_answer_time = reduce(
        reducer_answer_time, mapped_answer_time, [])

    # sum of the timedeltas for the calculation of the average
    answer_time_sum = reduce(reducer_answer_time, reduced_answer_time, 0)
    logger.info(" El promedio en dias de respuesta de post es de "
                f"{(answer_time_sum/(len(reduced_answer_time)))/3600/24}")
