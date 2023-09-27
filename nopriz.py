import asyncio
import logging
from datetime import datetime

from aiohttp import ClientSession


class ScraperNopriz:
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                             '(KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}

    def __init__(self):
        self.session = None
        self.dt_format = None
        self.date_from = None
        self.date_to = None

    @classmethod
    async def create(
            cls,
            dt_format: str,
            date_from: datetime = None,
            date_to: datetime = None,
    ):
        self = ScraperNopriz()
        self.session = await ClientSession().__aenter__()
        self.dt_format = dt_format
        self.date_from = date_from
        self.date_to = date_to
        return self

    async def collect_ids(self):
        ids = set()
        collect_per_page = 5000
        json_ = {
            'filters': {},
            'page': 1,
            'pageCount': f"{collect_per_page}",
            'searchString': "",
            'sortBy': {
                'registry_registration_date': 'DESC'
            }
        }
        while True:
            async with self.session.post(
                    'https://reestr.nopriz.ru/api/sro/all/member/list',
                    headers=self.headers,
                    json=json_
            ) as r:
                r_json = await r.json()
                for d in r_json['data']['data']:
                    dt = datetime.strptime(d['registry_registration_date'].replace('+03:00', ''), '%Y-%m-%dT%H:%M:%S')
                    if dt < self.date_from:
                        logging.info(f'Page №: {json_["page"]}, last date: {dt}')
                        logging.info(f'{len(ids)} ids was collected')
                        return list(ids)
                    elif dt > self.date_to:
                        continue
                    else:
                        print(f'{dt=} | {self.date_from=}')
                        ids.add(d['id'])
                else:
                    logging.info(f'Page №: {json_["page"]}, last date: {dt}')
                    logging.info(f'Page №: {json_["page"]} ; Collected ids: {len(ids)}')

            if len(r_json['data']['data']) < collect_per_page:
                break
            json_["page"] += 1
        return list(ids)

    async def collect_page_info(self, member_id: int):
        url = f'https://reestr.nopriz.ru/api/member/{member_id}/info'
        async with self.session.post(url, headers=ScraperNopriz.headers) as r:
            r = await r.json()
            r = r['data']
        try:
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
                'Регистрационный номер члена СРО': r.get('inventory_number'),
                'Полное наименование': r.get('full_description'),
                'Сокращенное наименование': r.get('short_description'),
                'Тип члена СРО': r['member_type'].get('title') if r.get('member_type') else None,
                'Дата регистрации в реестре': datetime.strptime(
                        r['registry_registration_date'], '%Y-%m-%dT%X%z'
                ).strftime(f'{self.dt_format}') if r.get('registry_registration_date') else None,
                'Дата и номер решения о приеме в члены': r.get('basis'),
                'Дата вступления в силу решения о приеме': datetime.strptime(
                    r['approved_basis_date'], '%Y-%m-%dT%X%z'
                ).strftime(f'{self.dt_format}') if r.get('approved_basis_date') else None,
                'ОГРН/ОГРНИП': r.get('ogrnip'),
                'ИНН': r.get('inn'),
                'Номер контактного телефона': r.get('phones'),
                'Адрес места нахождения юридического лица': full_address,
                'ФИО, осуществляющего функции единоличного исполнительного органа юридического лица и (или) руководителя'
                ' коллегиального исполнительного органа юридического лица': r.get('director'),
                'Статус члена СРО': r['member_status'].get('title') if r.get('member_status') else None,
                'Сведения о соответствии члена СРО условиям членства в СРО, предусмотренным законодательством РФ и (или)'
                ' внутренними документами СРО': r['accordance_status'].get('title') if r.get('accordance_status') else None,
                'Дата создания': datetime.strptime(
                    r['created_at'], '%Y-%m-%dT%X%z'
                ).strftime(f'{self.dt_format}') if r.get('created_at') else None,
                'Обновлено': datetime.strptime(
                    r['last_updated_at'], '%Y-%m-%dT%X%z'
                ).strftime(f'{self.dt_format} %X') if r.get('last_updated_at') else None,
                'Дата прекращения членства': datetime.strptime(
                    r['suspension_date'], '%Y-%m-%dT%X%z'
                ).strftime(f'{self.dt_format} %X') if r.get('suspension_date') else None,
                'Основание прекращения членства': r.get('suspension_reason'),
                'Размер взноса в компенсационный фонд возмещения вреда':
                    r['member_right_vv'].get('compensation_fund') if r.get('member_right_vv') else None,
                'Размер взноса в компенсационный фонд обеспечения договорных обязательств':
                    r['member_right_odo'].get('compensation_fund') if r.get('member_right_odo') else None
             }
        except Exception:
            logging.error(f'id:{member_id}, inn: {r.get("inn")}', exc_info=True)
            raise

    async def collect_data(self) -> list[dict]:
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
