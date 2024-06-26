import time
from abc import ABC, abstractmethod
import logging
import asyncio
from datetime import datetime
from pathlib import Path

from aiohttp import ClientSession
from tenacity import retry, before_sleep_log, wait_random

from src.schemas import FiltersNostroy, BaseRow
from config import DATE_FORMAT


class BaseScrapper(ABC):
    date_format: str = DATE_FORMAT
    last_ip_change: float | None = None

    def __init__(self, date_from: datetime, date_to: datetime,
                 date_format: str | None = None, proxy_url: str | None = None):
        if date_format:
            self.date_format = date_format

        self.date_from = date_from
        self.date_to = date_to
        self.proxy_url = proxy_url

    @abstractmethod
    async def _collect_ids(self, filters: dict) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    async def collect_page_info(self, id_: int) -> dict[str, int | str]:
        raise NotImplementedError

    async def collect_ids(self, filters: dict) -> list[int]:
        ids = []

        if all([not isinstance(v, (list, tuple)) for v in filters.values()]):
            ids += await self._collect_ids(filters)
        else:
            for k, v in filters.items():
                if isinstance(v, (list, tuple)):
                    for v_ in v:
                        filters_ = filters.copy()
                        filters_[k] = v_
                        logging.info(f'Start using {filters_=}')
                        ids += await self._collect_ids(filters_)
        return ids

    async def get_ids(self, filters: FiltersNostroy | None = None) -> list[int]:
        filters = filters.dict(exclude_none=True) if filters else {}
        # filepath = Path(f'ids_{self.__class__.__name__}.txt')
        filepath = Path('not_appended_ids.txt')

        if filepath.exists():
            with open(filepath, 'r') as f:
                ids = [int(x.replace('\n', '')) for x in f.readlines()]
                print(len(ids), len(set(ids)))
                ids = sorted(list(set(ids)))
            logging.info(f'Red {len(ids)} ids')
        else:
            ids = await self.collect_ids(filters=filters)
            with open(filepath, 'w') as f:
                f.writelines([f'{x}\n' for x in ids])
            logging.info(f'Wrote {len(ids)} to {filepath}')

        # ids = ids[90000:]  # FIXME
        return ids

    async def collect_data(self, ids: list[int]) -> list:
        tt = 0
        tasks = []
        for id_ in ids:
            tasks.append(self.collect_page_info(id_))
            if len(tasks) % 50 == 0 or id_ == ids[-1]:
                res = await asyncio.gather(*tasks)
                tt += len(res)
                logging.info(f'Collected: {tt} pages info')
                tasks = []
                yield res

    @retry(
        wait=wait_random(5, 10),
        # before_sleep=before_sleep_log(logger=logging.getLogger(), log_level=logging.INFO),
        # before=before_log(logger=logging.getLogger(), log_level=logging.INFO),
        # after=after_log(logger=logging.getLogger(), log_level=logging.INFO),
    )
    async def request_json(self, method: str, url: str, **kwargs):
        # await asyncio.sleep(random.randint(3, 6))
        try:
            async with self._session.request(method=method, url=url, timeout=5, **kwargs) as r:
                return await r.json()
        except Exception as ex:
            async with self._session.request(
                    method='GET',
                    url='http://node-de-71.astroproxy.com:10509/api/changeIP?apiToken=2de5dfea6b2c1e52'
            ) as resp:
                assert resp.ok, resp
            raise ex

    def get_filename(self, service_name: str) -> str:
        return f'{service_name}_from_{self.date_from.date()}_to_{self.date_to.date()}'

    async def __aenter__(self):
        self._session = await ClientSession().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()
