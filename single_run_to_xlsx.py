import asyncio
import logging
import time
from datetime import datetime
from pathlib import Path

from openpyxl import Workbook

from config import DATE_FORMAT
from src.scrappers.nopriz import ScraperNopriz
from src.scrappers.nostroy import ScraperNostroy
from src.my_logging import get_logger
from src.schemas import NostroyRow, NoprizRow

DATE_FROM = '01.01.2024'
DATE_TO = datetime.now().date().strftime(DATE_FORMAT)


def write_data_to_excel(filepath: Path, data: list[NoprizRow | NostroyRow]) -> None:
    wb = Workbook()
    ws = wb.worksheets[0]

    i = 0
    while True:
        try:
            columns = list(data[0].dict(by_alias=True).keys())
            ws.append(columns)

            for r in data:
                d = r.dict(by_alias=True)
                ws.append(list(d.values()))

            wb.save(filepath)
            logging.info(f'{filepath} was written')
            break
        except PermissionError as ex:
            logging.info('Please close the file')
            i += 1
            time.sleep(10)
            if i == 5:
                raise ex


async def main():
    date_from = datetime.strptime(DATE_FROM, DATE_FORMAT)
    date_to = datetime.strptime(DATE_TO, DATE_FORMAT)

    async with ScraperNopriz(date_format=DATE_FORMAT, date_from=date_from, date_to=date_to) as scrapper:
        data = await scrapper.collect_data()
        filepath = Path(scrapper.get_filename('nopriz')).with_suffix('.xlsx')
        write_data_to_excel(filepath, data)

    async with ScraperNostroy(date_format=DATE_FORMAT, date_from=date_from, date_to=date_to) as scrapper:
        data = await scrapper.collect_data()
        filepath = Path(scrapper.get_filename('nostroy')).with_suffix('.xlsx')
        write_data_to_excel(filepath, data)


if __name__ == '__main__':
    get_logger(Path(__file__).stem + '.log')

    try:
        asyncio.run(main())
    except Exception as ex:
        logging.error(ex, exc_info=True)
        exit(1)
