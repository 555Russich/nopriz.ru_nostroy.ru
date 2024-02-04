import logging
import asyncio
from datetime import datetime

from config import DATE_FORMAT, URL_SPREADSHEET
from src.my_logging import get_logger
from src.scrappers.nopriz import ScraperNopriz
from src.scrappers.nostroy import ScraperNostroy
from src.my_google import GoogleSheets
from src.schemas import NoprizRow, NostroyRow

DATE_FROM = '01.02.2024'
DATE_TO = datetime.now().date().strftime(DATE_FORMAT)


async def main():



async def scrap_all():
    date_format = DATE_FORMAT
    date_from = datetime.strptime(DATE_FROM, DATE_FORMAT)
    date_to = datetime.strptime(DATE_TO, DATE_FORMAT)

    async with GoogleSheets() as gs:
        await gs.open_spreadsheet(URL_SPREADSHEET)

        logging.info(f'Start scraping nopriz')
        async with ScraperNopriz(date_format=date_format, date_from=date_from, date_to=date_to) as scrapper:
            data = await scrapper.collect_data()
            await gs.get_or_add_worksheet(0, name='nopriz')
            await gs.fill_new_rows(data, row_model=NoprizRow)

        logging.info(f'Start scraping nostroy')
        async with ScraperNostroy(date_format=date_format, date_from=date_from, date_to=date_to) as scrapper:
            data = await scrapper.collect_data()
            await gs.get_or_add_worksheet(1, name='nostroy')
            await gs.fill_new_rows(data, row_model=NostroyRow)


if __name__ == "__main__":
    get_logger('nopriz_nostroy.log')

    try:
        asyncio.run(main())
    except Exception as ex:
        logging.error(ex, exc_info=True)
        exit(1)
