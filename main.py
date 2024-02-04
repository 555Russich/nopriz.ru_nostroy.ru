import logging
import asyncio
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import DATE_FORMAT, URL_SPREADSHEET
from src.my_logging import get_logger
from src.scrappers.nopriz import ScraperNopriz
from src.scrappers.nostroy import ScraperNostroy
from src.my_google import GoogleSheets
from src.schemas import NoprizRow, NostroyRow

DATE_FROM = '01.02.2024'
DATE_TO = datetime.now().date().strftime(DATE_FORMAT)


async def main():
    scheduler = AsyncIOScheduler(logger=logging.getLogger())
    scheduler.add_job(
        func=scrap_all,
        trigger='cron',
        hour='9-18',  # UTC+3
        # minute=0,
        # second=30,
        max_instances=1,
    )
    scheduler.start()



async def scrap_all(
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        date_format: str = DATE_FORMAT
) -> None:
    if date_to is None:
        date_to = datetime.now()

    if date_from is None:
        date_from = date_to - timedelta(days=1)
        date_from.replace(hour=0, minute=0, second=0, microsecond=0)

    logging.info(f'date_from={date_from} | date_to={date_to}')

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
        loop = asyncio.new_event_loop()
        loop.create_task(main())
        loop.run_forever()
    except Exception as ex:
        logging.error(ex, exc_info=True)
        exit(1)
