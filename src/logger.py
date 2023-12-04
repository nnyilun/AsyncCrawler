import datetime
import logging
from config import config

current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_path = config.get("log_path")

DEBUG = logging.DEBUG
INFO = logging.INFO
ERROR = logging.ERROR

logging.basicConfig(
    filename=f'{log_path}log_{current_time}.txt',
    format = '%(asctime)s - %(levelname)s - %(funcName)s - %(message)s',
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info("logger running...")