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
from src.schemas import NostroyRow, NoprizRow, FiltersNostroy

DATE_FROM = '01.01.1900'
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
    data = []

    # async with ScraperNopriz(date_format=DATE_FORMAT, date_from=date_from, date_to=date_to) as scrapper:
    #     data += await scrapper.collect_data()
    # filepath = Path(scrapper.get_filename('nopriz')).with_suffix('.xlsx')
    # write_data_to_excel(filepath, data)

    sro_list = [
        'СРО-С-046-06102009',
        'СРО-С-083-27112009',
        'СРО-С-103-07122009',
        'СРО-С-109-11122009',
        'СРО-С-146-24122009',
        'СРО-С-146-24122009',
        'СРО-С-166-30122009',
        'СРО-С-189-01022010',
        'СРО-С-228-20072010'
    ]
    filters = FiltersNostroy(region_number=47, sro_registration_number=sro_list, member_status=1)
    # filters = {}

    async with ScraperNostroy(date_format=DATE_FORMAT, date_from=date_from, date_to=date_to) as scrapper:
        async with scrapper._session.get('https://icanhazip.com') as resp:
            print(await resp.text())
        data += await scrapper.collect_data(filters=filters)

    filepath = Path(scrapper.get_filename('nostroy')).with_suffix('.xlsx')
    write_data_to_excel(filepath, data)


if __name__ == '__main__':
    get_logger(Path(__file__).stem + '.log')

    try:
        asyncio.run(main())
    except Exception as ex:
        logging.error(ex, exc_info=True)
        exit(1)
