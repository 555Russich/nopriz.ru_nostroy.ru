import logging
import sys
from datetime import datetime

from src.date_utils import TZ_MSC


def get_logger(filename):
    logging.basicConfig(
        level=logging.INFO,
        encoding='utf-8',
        format="[{asctime}]:[{levelname}]:{message}",
        style='{',
        handlers=[
            logging.FileHandler(filename, mode='w'),
            logging.StreamHandler(sys.stdout),
        ]
    )
    logging.Formatter.converter = lambda *args: datetime.now(tz=TZ_MSC).timetuple()
