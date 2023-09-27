import asyncio
from datetime import datetime
import logging

from aiohttp import ClientSession

from my_logging import get_logger

get_logger('nosstroy.log')
DATE_FORMAT = '%d.%m.%y'
DATE_FROM = '01.01.23'
DATE_TO = datetime.now().date().strftime(DATE_FORMAT)
# print(DATE_TO)
# exit()


class ScraperNostroy:
    @classmethod
    async def create(
            cls,
            dt_format: str,
            date_from: datetime = None,
            date_to: datetime = None,
    ):
        self = ScraperNostroy()
        self.session = await ClientSession().__aenter__()
        self.dt_format = dt_format
        self.date_from = date_from
        self.date_to = date_to
        return self

    async def collect_data(self):
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

    async def collect_ids(self) -> list:
        ids = set()
        data = {
            'filters': {},
            'page': 1,
            'pageCount': "1000",
            "sortBy": {
                'registry_registration_date': "DESC"  # sorted from new to old registrations
            }
        }
        while True:
            async with self.session.post(
                    'https://api-open-nostroy.anonamis.ru/api/sro/all/member/list',
                    json=data
            ) as r:
                r_json = await r.json()
                logging.info(f'{len(ids)} collected')
                for d in r_json['data']['data']:
                    dt = datetime.strptime(d['registry_registration_date_time_string'], '%d.%m.%Y %H:%M:%S')
                    if dt < self.date_from:
                        logging.info(f'{len(ids)} ids collected until {DATE_FROM}')
                        return list(ids)
                    elif dt > self.date_to:
                        continue
                    else:
                        ids.add(d['id'])
                else:
                    logging.info(f"last date page {data['page']}: {dt}")
                    data['page'] += 1


    async def collect_page_info(self, id_: str) -> dict:
        url = f'https://api-open-nostroy.anonamis.ru/api/member/{id_}/info'
        async with self.session.post(url) as r:
            r = await r.json()
            r = r['data']

            address_info = [
                r.get('index'),
                r.get('country'),
                r.get('subject'),
                r.get('district'),
                r.get('locality'),
                r.get('street'),
                r.get('house'),
                r.get('building'),
                r.get('room')
            ]
            full_address = ', '.join([info for info in address_info if info is not None])

            return {
                'СРО': r['sro'].get('full_description') if r.get('sro') else None,
                'Полное наименование': r.get('full_description'),
                'Сокращенное наименование': r.get('short_description'),
                'Регистрационный номер члена СРО': r.get('registration_number'),
                'ОГРН/ОГРНИП': r.get('ogrnip'),
                'ИНН': r.get('inn'),
                'Номер контактного телефона': r.get('phones'),
                'Регион:': r.get('district'),
                'Адрес места нахождения юридического лица': full_address,
                'ФИО, осуществляющего функции единоличного исполнительного органа юридического лица и (или) руководителя'
                ' коллегиального исполнительного органа юридического лица': r.get('director'),
                'Сведения о соответствии члена СРО условиям членства в СРО, предусмотренным законодательством РФ и (или)'
                ' внутренними документами СРО': r['accordance_status'].get('title') if r.get('accordance_status') else None,
                'Тип члена СРО': r['member_type'].get('title') if r.get('member_type') else None,
                'Дата размещения информации': datetime.strptime(
                        r['registry_registration_date'], '%Y-%m-%dT%X%z'
                ).strftime(DATE_FORMAT) if r.get('registry_registration_date') else None,
                'Дата изменения информации': datetime.strptime(
                    r['last_updated_at'], '%Y-%m-%dT%X%z'
                ).strftime(f'{DATE_FORMAT} %X') if r.get('last_updated_at') else None,
             }
