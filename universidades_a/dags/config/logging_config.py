import logging
import logging.config
from pathlib import Path

import yaml


def setup_logging(default_level=logging.INFO):
    '''
    Logging setup , the configuration parameters are taken
    from the yaml file that works as a dictionary

        Returns:
                    (str): configuration result message
    '''

    try:
        logging_config_path = Path(
            'airflow/universidades_a/dags/config/logging_config.yaml')
        with open(logging_config_path, 'rt') as file:
            config = yaml.safe_load(file.read())
            logging.config.dictConfig(config)
            return ('Configuracion de Logging exitosa')
    except Exception as error:
        print(error)
        logging.basicConfig(level=default_level)
        return ('Error en configuracion de Logging, '
                'se utilizara configuración por defecto')
