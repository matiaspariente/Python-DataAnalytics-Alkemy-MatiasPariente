import logging
import logging.config
from pathlib import Path

root_path = Path(__file__).parents[1]
logging_config_path = Path(
            root_path,
            'datos_a/config/logging.cfg')

logging.config.fileConfig(logging_config_path)

logger = logging.getLogger("analyzer")

logger.info("Logging Test")
