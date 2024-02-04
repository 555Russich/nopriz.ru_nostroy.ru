from abc import ABC, abstractmethod
import logging
import asyncio
from datetime import datetime

from aiohttp import ClientSession


class BaseScrapper(ABC):
    def __init__(self, date_format: str, date_from: datetime, date_to: datetime):
        self.date_format = date_format
        self.date_from = date_from
        self.date_to = date_to

    @abstractmethod
    async def collect_ids(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    async def collect_page_info(self, id_: str) -> dict[str, int | str]:
        raise NotImplementedError

    async def collect_data(self) -> list:
        data = []

        ids = await self.collect_ids()
        tasks = []
        for id_ in ids:
            tasks.append(self.collect_page_info(id_))
            if len(tasks) % 20 == 0 or id_ == ids[-1]:
                data += await asyncio.gather(*tasks)
                logging.info(f'Collected: {len(data)} pages info')
                tasks = []

        return data

    def get_filename(self, service_name: str) -> str:
        return f'{service_name}_from_{self.date_from.date()}_to_{self.date_to.date()}'

    async def __aenter__(self):
        self._session = await ClientSession().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()
