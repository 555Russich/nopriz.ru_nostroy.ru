import asyncio
from datetime import datetime, timedelta

from main import scrap_all
from src.my_logging import get_logger


async def main():
    date_to = datetime.now()
    date_from = date_to - timedelta(days=5)
    # date_from = datetime(year=2024, month=3, day=24)
    await scrap_all(date_from=date_from, date_to=date_to)


if __name__ == '__main__':
    get_logger('single_run_to_spreadsheet.log')
    asyncio.run(main())
