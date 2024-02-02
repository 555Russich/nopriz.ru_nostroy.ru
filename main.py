import asyncio
from datetime import datetime
from pathlib import Path
import logging
import time

import pandas

from src.nopriz import ScraperNopriz
from src.nosstroy import ScraperNostroy
from src.my_logging import get_logger

DATE_FORMAT = '%d.%m.%Y'
DATE_FROM = '01.12.2023'
DATE_TO = datetime.now().date().strftime(DATE_FORMAT)
# DATE_TO = '30.09.2023'


def write_data_to_excel(filename, data):
    df = pandas.DataFrame(data)
    i = 0
    while True:
        try:
            df.to_excel(filename, index=False)
            logging.info(f'{filename} was written')
            break
        except PermissionError as ex:
            logging.info('Please close the file')
            i += 1
            time.sleep(10)
            if i == 5:
                raise ex

async def main():
    scraper_nopriz = await ScraperNopriz.create(
        dt_format=DATE_FORMAT,
        date_from=datetime.strptime(DATE_FROM, DATE_FORMAT),
        date_to=datetime.strptime(DATE_TO, DATE_FORMAT)
    )
    data = await scraper_nopriz.collect_data()
    FILEPATH_XLSX = Path(f'data_nopriz_from_{DATE_FROM}_to_{DATE_TO}.xlsx')
    write_data_to_excel(FILEPATH_XLSX, data)
    await scraper_nopriz.session.close()

    scraper_nostroy = await ScraperNostroy.create(
        dt_format=DATE_FORMAT,
        date_from=datetime.strptime(DATE_FROM, DATE_FORMAT),
        date_to=datetime.strptime(DATE_TO, DATE_FORMAT)
    )
    data = await scraper_nostroy.collect_data()
    FILEPATH_XLSX = Path(f'data_nostroy_from_{DATE_FROM}_to_{DATE_TO}.xlsx')
    write_data_to_excel(FILEPATH_XLSX, data)
    await scraper_nostroy.session.close()


if __name__ == '__main__':
    get_logger('main.log')
    asyncio.run(main())
