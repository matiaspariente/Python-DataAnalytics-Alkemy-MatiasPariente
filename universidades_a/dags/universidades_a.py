import logging
from datetime import date, datetime, timedelta
from pathlib import Path

from decouple import config
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

default_args = {
    'email': ['matiaspariente@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    "schedule_interval": '@hourly'
}


def age(born):
    '''
    Function that receives a date and returns the current age

    Returns:
                    (int): current age
    '''
    born = datetime.strptime(born, "%Y-%m-%d").date()
    today = date.today()
    return today.year - born.year - (
        (today.month, today.day)
        < (born.month, born.day))


def transform_flores():
    '''
    Function that will normalize the data of the Universidad de Villa Maria.
    Raw data is extracted from the corresponding csv file and through
    Pandas that information is processed as required.

    Returns:
                    (str): normalization result message
    '''
    try:
        filepath_universidades_a = Path(__file__).parents[1]
        filepath_flores_csv = Path(
            filepath_universidades_a,
            'dags/files/universidad_flores.csv')
        filepath_cpaloc_csv = Path(
            filepath_universidades_a,
            'dags/files/codigos_postales.csv')
        df_cpaloc = pd.read_csv(filepath_cpaloc_csv, sep=',')
        df_flores = pd.read_csv(filepath_flores_csv, sep=',')
        # column direccion not required for processing
        df_flores = df_flores.drop('direccion', axis=1)
        # location is added to the table by doing a merge
        # through the postal code
        df_flores = df_flores.merge(df_cpaloc, on='codigo_postal')
        # the corresponding columns are modified to lowercase
        # and the spaces are eliminated
        columns_tolower = [
            'universidad', 'carrera', 'name',
            'correo_electronico', 'localidad']
        for i in range(len(columns_tolower)):
            df_flores[columns_tolower[i]] = df_flores[
                columns_tolower[i]].str.lower()
            df_flores[columns_tolower[i]] = df_flores[
                columns_tolower[i]].str.strip(' ')
        df_flores['sexo'] = df_flores['sexo'].replace(
            ['F', 'M'], ['female', 'male'], regex=True)
        # the suffixes are eliminated from the name column and
        # then they are separated by first and last name,
        # only 1 last name is taken
        df_flores['name'] = df_flores['name'].str.lstrip('mrs. ')
        df_flores['name'] = df_flores['name'].str.lstrip('ms. ')
        df_flores['name'] = df_flores['name'].str.lstrip('mr. ')
        name = df_flores['name'].str.split(expand=True)
        name = name.drop([2, 3], axis=1)
        name.columns = ['first_name', 'last_name']
        df_flores = pd.concat([df_flores, name], axis=1)
        df_flores = df_flores.drop('name', axis=1)
        # age is calculated through the Age function
        df_flores['age'] = df_flores['fecha_nacimiento'].apply(age)
        df_flores = df_flores.drop('fecha_nacimiento', axis=1)
        # columns are renamed and ordered as required
        df_flores = df_flores.set_axis(
            ['university', 'career', 'inscription_date', 'gender',
                'postal_code', 'email', 'location', 'first_name',
                'last_name', 'age'], axis=1)
        df_flores = df_flores.reindex(columns=[
                    'university', 'career', 'inscription_date',
                    'first_name', 'last_name', 'gender', 'age',
                    'postal_code', 'location', 'email'])
        # the type of postal code is modified as required
        df_flores['postal_code'] = df_flores['postal_code'].astype('object')
        # directory is generated and the file is saved
        # with the normalizations
        filepath_flores_txt = Path(
            filepath_universidades_a,
            'dags/files/universidad_flores.txt')
        filepath_flores_txt.parent.mkdir(parents=True, exist_ok=True)
        df_flores.to_csv(filepath_flores_txt, index=False)
    except Exception as exc:
        return exc
    finally:
        return "success"


def extract_sql():
    '''
    Universities Data is extracted from the database with the saved queries.
    This Data is saved in a csv file corresponding to each universities
    '''
    filepath_universidades_a = Path(__file__).parents[1]
    logger = logging.getLogger("Extract")
    try:
        engine = create_engine(
            "postgresql://{}:{}@{}:{}/{}"
            .format(
                config('_PG_USER'),
                config('_PG_PASSWD'),
                config('_PG_HOST'),
                config('_PG_PORT'),
                config('_PG_DB')))
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        if "port" in error:
            logger.critical("Comunication Error, verify HOST:PORT")
        else:
            logger.critical("Autentication error, verify User / password")
    else:
        logger.info("Database connection success")

    try:
        filepath_flores = Path(
            filepath_universidades_a,
            'sql/flores.sql')
        filepath_villamaria = Path(
            filepath_universidades_a,
            'sql/villaMaria.sql')
        with open(filepath_flores, 'r', encoding="utf-8") as file:
            query_flores = file.read()
        with open(filepath_villamaria, 'r', encoding="utf-8") as file:
            query_villamaria = file.read()
        df_flores = pd.read_sql(query_flores, engine)
        df_villamaria = pd.read_sql(query_villamaria, engine)
    except IOError:
        logger.error("SQL file not appear or exist")
    else:
        logger.info("SQL query reading success")

    try:
        filepath_flores_csv = Path(
            filepath_universidades_a,
            'dags/files/universidad_flores.csv')
        filepath_flores_csv.parent.mkdir(parents=True, exist_ok=True)
        filepath_villamaria_csv = Path(
            filepath_universidades_a,
            'dags/files/universidad_villamaria.csv')
        filepath_villamaria_csv.parent.mkdir(parents=True, exist_ok=True)
        df_flores.to_csv(filepath_flores_csv, index=False)
        df_villamaria.to_csv(filepath_villamaria_csv, index=False)
    except Exception as exc:
        logger.error(exc)
    else:
        logger.info("csv files were generated successfully")


def transform_villamaria():
    '''
    Raw data is extracted from the corresponding csv file and through
    Pandas that information is processed as required.

    Returns:
                    (str): normalization result message
    '''
    try:
        filepath_universidades_a = Path(__file__).parents[1]
        filepath_villamaria_csv = Path(
            filepath_universidades_a,
            'dags/files/universidad_villamaria.csv')
        filepath_cpaloc_csv = Path(
            filepath_universidades_a,
            'dags/files/codigos_postales.csv')
        df_cpaloc = pd.read_csv(filepath_cpaloc_csv, sep=',')
        # puplicate locations that refer to more than one postl code should
        # be eliminated for not having more information regarding the location
        df_cpaloc = df_cpaloc.drop_duplicates(subset=['localidad'])
        df_villamaria = pd.read_csv(filepath_villamaria_csv, sep=',')
        # column direccion not required for processing
        df_villamaria = df_villamaria.drop('direccion', axis=1)
        # the corresponding columns are modified underscore to
        # spaces as required
        column_to_normalize = ['universidad', 'carrera', 'nombre', 'localidad']
        for i in range(len(column_to_normalize)):
            df_villamaria[column_to_normalize[i]] = df_villamaria[
                column_to_normalize[i]].replace('_', ' ', regex=True)
        # postal_code is added to the table by doing a merge
        # through the location
        df_villamaria = df_villamaria.merge(df_cpaloc, on='localidad')
        # the corresponding columns are modified to lowercase
        # and the spaces are eliminated
        columns_tolower = [
            'universidad', 'carrera', 'nombre', 'email', 'localidad']
        for i in range(len(columns_tolower)):
            df_villamaria[columns_tolower[i]] = df_villamaria[
                columns_tolower[i]].str.lower()
            df_villamaria[columns_tolower[i]] = df_villamaria[
                columns_tolower[i]].str.strip(' ')
        # format of the dates is modified as required
        df_villamaria['fecha_de_inscripcion'] = pd.to_datetime(
            df_villamaria['fecha_de_inscripcion']).dt.strftime('%Y-%m-%d')
        df_villamaria['fecha_nacimiento'] = pd.to_datetime(
            df_villamaria['fecha_nacimiento']).dt.strftime('%Y-%m-%d')
        # age is calculated through the Age function
        df_villamaria['age'] = df_villamaria['fecha_nacimiento'].apply(age)
        df_villamaria = df_villamaria.drop('fecha_nacimiento', axis=1)
        df_villamaria['sexo'] = df_villamaria['sexo'].replace(
            ['F', 'M'], ['female', 'male'], regex=True)
        # the suffixes are eliminated from the name column and
        # then they are separated by first and last name,
        # only 1 last name is taken
        df_villamaria['nombre'] = df_villamaria['nombre'].str.lstrip('mrs. ')
        df_villamaria['nombre'] = df_villamaria['nombre'].str.lstrip('ms. ')
        df_villamaria['nombre'] = df_villamaria['nombre'].str.lstrip('mr. ')
        name = df_villamaria['nombre'].str.split(expand=True)
        name = name.drop([2, 3], axis=1)
        name.columns = ['first_name', 'last_name']
        df_villamaria = pd.concat([df_villamaria, name], axis=1)
        df_villamaria = df_villamaria.drop('nombre', axis=1)
        # columns are renamed and ordered as required
        df_villamaria = df_villamaria.set_axis(
            ['university', 'career', 'inscription_date', 'gender',
                'location', 'email', 'postal_code', 'age', 'first_name',
                'last_name'], axis=1)
        df_villamaria = df_villamaria.reindex(columns=[
                    'university', 'career', 'inscription_date',
                    'first_name', 'last_name', 'gender', 'age',
                    'postal_code', 'location', 'email'])
        # the type of postal code is modified as required
        df_villamaria['postal_code'] = df_villamaria[
            'postal_code'] .astype('object')
        # directory is generated and the file is saved
        # with the normalizations
        filepath_villamaria_txt = Path(
            filepath_universidades_a,
            'dags/files/universidad_villamaria.txt')
        filepath_villamaria_txt.parent.mkdir(parents=True, exist_ok=True)
        df_villamaria.to_csv(filepath_villamaria_txt, index=False)
    except Exception as exc:
        return exc
    finally:
        return "success"


def transform_data():
    '''
    Universities Data is extracted from the csv files generated in extract task
    These data are normalized as required through the transform functions
    '''
    logger = logging.getLogger("Transform")
    res = transform_flores()
    if(res == "success"):
        logger.info(
            "Universidad de Flores normalized txt were generate succesfully")
    else:
        logger.error(res)
    res = transform_villamaria()
    if(res == "success"):
        logger.info(
            "Universidad de Villa María"
            "normalized txt were generate succesfully")
    else:
        logger.error(res)


def load_data_flores():

    # function that will upload to S3 universidad de flores transform data
    pass


def load_data_villamaria():

    # function that will upload to S3 universida de Villa Maria transform data
    pass


# Initialization of the DAG for the etl process for universidades_a
with DAG(
    'ETL_universidades_a',
    description='ETL DAG for Universidad de Flores and Villa Maria',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 5, 31)
) as dag:

    # Operator that will extract data with SQL querys
    extract = PythonOperator(
        task_id='extract_sql_task',
        python_callable=extract_sql
        )

    # Operator that will process data with Pandas
    transform = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data
        )

    # Operator that will upload to S3 universidad de flores transform data
    load_flores = PythonOperator(
        task_id='load_data_flores_task',
        python_callable=load_data_flores
        )

    # Operator that will upload to S3 universida de Villa Maria transform data
    load_villamaria = PythonOperator(
        task_id='load_data_villamaria_task',
        python_callable=load_data_villamaria
        )

    extract >> transform >> [load_flores, load_villamaria]
