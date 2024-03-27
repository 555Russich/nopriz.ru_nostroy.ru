import logging
from datetime import datetime

from src.scrappers.base import BaseScrapper
from src.schemas import NoprizRow


class ScraperNopriz(BaseScrapper):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                             '(KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}

    async def _collect_ids(self, filters: dict):
        ids = set()
        collect_per_page = 1000
        json_ = {
            'filters': filters,
            'page': 1,
            'pageCount': f"{collect_per_page}",
            'searchString': "",
            'sortBy': {
                'registry_registration_date': 'DESC'
            }
        }
        while True:
            async with self._session.post(
                    'https://reestr.nopriz.ru/api/sro/all/member/list',
                    headers=self.headers,
                    json=json_
            ) as r:
                r_json = await r.json()
                for d in r_json['data']['data']:
                    dt = datetime.strptime(
                        d['registry_registration_date'].replace('+03:00', ''),
                        '%Y-%m-%dT%H:%M:%S'
                    )
                    if dt < self.date_from:
                        logging.info(f'Page №: {json_["page"]}, last date: {dt}')
                        logging.info(f'{len(ids)} ids was collected')
                        return list(ids)
                    elif dt > self.date_to:
                        continue
                    else:
                        ids.add(d['id'])
                else:
                    logging.info(f'Page №: {json_["page"]}, last date: {dt}')
                    logging.info(f'Page №: {json_["page"]} ; Collected ids: {len(ids)}')

            if len(r_json['data']['data']) < collect_per_page:
                break
            json_["page"] += 1
        return list(ids)

    async def collect_page_info(self, id_: int) -> NoprizRow:
        async with self._session.post(f'https://reestr.nopriz.ru/api/member/{id_}/info', headers=self.headers) as r:
            r = await r.json()
            r = r['data']

        return NoprizRow(
            id=r['id'],
            sro=r['sro'].get('full_description'),
            registration_number=NoprizRow.parse_registration_number(r),
            full_description=r.get('full_description'),
            short_description=r.get('short_description'),
            member_type=r['member_type'].get('title') if r.get('member_type') else None,
            registry_registration_date=datetime.strptime(
                r['registry_registration_date'], '%Y-%m-%dT%X%z'
            ).strftime(f'{self.date_format}') if r.get('registry_registration_date') else None,
            basis=r.get('basis'),
            approved_basis_date=datetime.strptime(
                r['approved_basis_date'], '%Y-%m-%dT%X%z'
            ).strftime(f'{self.date_format}') if r.get('approved_basis_date') else None,
            ogrnip=r.get('ogrnip'),
            inn=r.get('inn'),
            phones=r.get('phones'),
            full_address=NoprizRow.parse_full_address(r),
            director=r.get('director'),
            member_status=r['member_status'].get('title') if r.get('member_status') else None,
            accordance_status=r['accordance_status'].get('title') if r.get('accordance_status') else None,
            created_at=datetime.strptime(
                r['created_at'], '%Y-%m-%dT%X%z'
            ).strftime(f'{self.date_format}') if r.get('created_at') else None,
            last_updated_at=datetime.strptime(
                r['last_updated_at'], '%Y-%m-%dT%X%z'
            ).strftime(f'{self.date_format} %X') if r.get('last_updated_at') else None,
            suspension_date=datetime.strptime(
                r['suspension_date'], '%Y-%m-%dT%X%z'
            ).strftime(f'{self.date_format} %X') if r.get('suspension_date') else None,
            suspension_reason=r.get('suspension_reason'),
            member_right_vv=r['member_right_vv'].get('compensation_fund') if r.get('member_right_vv') else None,
            member_right_odo=r['member_right_odo'].get('compensation_fund') if r.get('member_right_odo') else None
        )
