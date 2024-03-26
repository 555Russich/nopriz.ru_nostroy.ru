from datetime import datetime
import logging

from src.scrappers.base import BaseScrapper
from src.schemas import NostroyRow


class ScraperNostroy(BaseScrapper):
    async def collect_ids(self) -> list[int]:
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
            async with self._session.post(
                    'https://reestr.nostroy.ru/api/sro/all/member/list',
                    json=data,
                    verify_ssl=False
            ) as r:
                r_json = await r.json()

                assert r_json['success'] is True, r_json['message']

                for d in r_json['data']['data']:
                    dt = datetime.strptime(d['registry_registration_date_time_string'], '%d.%m.%Y %H:%M:%S')
                    if dt < self.date_from:
                        logging.info(f'{len(ids)} ids collected until {self.date_from}')
                        return list(ids)
                    elif dt > self.date_to:
                        continue
                    else:
                        ids.add(d['id'])
                else:
                    logging.info(f"last date page {data['page']}: {dt}")
                    data['page'] += 1

                logging.info(f'{len(ids)} ids collected')

    async def collect_page_info(self, id_: str) -> NostroyRow:
        url = f'https://reestr.nostroy.ru/api/member/{id_}/info'
        async with self._session.post(url, verify_ssl=False) as r:
            r = await r.json()
            r = r['data']

            return NostroyRow(
                id=r['id'],
                sro=r['sro'].get('full_description'),
                full_description=r.get('full_description'),
                short_description=r.get('short_description'),
                registration_number=r.get('registration_number'),
                ogrnip=r.get('ogrnip'),
                inn=r.get('inn'),
                phones=r.get('phones'),
                region=r.get('district'),
                full_address=NostroyRow.parse_full_address(r),
                director=r.get('director'),
                accordance_status=r['accordance_status'].get('title') if r.get('accordance_status') else None,
                member_type=r['member_type'].get('title') if r.get('member_type') else None,
                registry_registration_date=datetime.strptime(
                    r['registry_registration_date'], '%Y-%m-%dT%X%z'
                ).strftime(self.date_format) if r.get('registry_registration_date') else None,
                last_updated_at=datetime.strptime(
                    r['last_updated_at'], '%Y-%m-%dT%X%z'
                ).strftime(f'{self.date_format} %X') if r.get('last_updated_at') else None
            )
