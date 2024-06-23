from datetime import datetime
import logging
import ssl

from src.scrappers.base import BaseScrapper
from src.schemas import NostroyRow

from config import FILEPATH_CERT

ssl_context = ssl.create_default_context(capath=FILEPATH_CERT)


class ScraperNostroy(BaseScrapper):
    NAME = 'nostroy'


    async def _collect_ids(self, filters: dict) -> list[int]:
        ids = set()
        data = {
            'filters': filters,
            'page': 1,
            'pageCount': 1000,
            "sortBy": {
                'registry_registration_date': "DESC"  # sorted from new to old registrations
            }
        }

        while True:
            url = 'https://reestr.nostroy.ru/api/sro/all/member/list'
            r_json = await self.request_json(method='POST', url=url, json=data, proxy=self.proxy_url, verify_ssl=False)

            assert r_json['success'] is True, r_json['message']

            if r_json['data']['count'] == 0:
                return list(ids)

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
            if len(r_json['data']['data']) < data['pageCount'] or data['page'] > r_json['data']['countPages']:
                return list(ids)

    async def get_sro_member(self, id_: str) -> NostroyRow:
        url = f'https://reestr.nostroy.ru/api/member/{id_}/info'
        r = await self.request_json(method='POST', url=url, verify_ssl=False, proxy=self.proxy_url)
        r = r['data']

        is_odo = None
        if r.get('right') and r['right']['is_odo']:
            is_odo = 'Действует без ограничений, в пределах фактического совокупного размера обязательств'

        return NostroyRow(
            id=r['id'],
            sro=r['sro'].get('full_description'),
            full_description=r.get('full_description'),
            short_description=r.get('short_description'),
            registration_number=r['registration_number'],
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
            ).strftime(f'{self.date_format} %X') if r.get('last_updated_at') else None,
            right_status=r['right']['right_status']['title'] if r.get('right') else None,
            right_basis=r['right']['basis'] if r.get('right') else None,
            is_simple=r['right']['is_simple'] if r.get('right') else None,
            is_extremely_dangerous=r['right']['is_extremely_dangerous'] if r.get('right') else None,
            is_nuclear=r['right']['is_nuclear'] if r.get('right') else None,
            is_odo=is_odo,
            responsibility_level_odo=f"{r['responsibility_level_odo']['title']}, "
                                     f"{r['responsibility_level_odo']['cost']}" if r.get('responsibility_level_odo') else None,
            responsibility_level_vv=f"{r['responsibility_level_vv']['title']}, "
                                    f"{r['responsibility_level_vv']['cost']}" if r.get('responsibility_level_vv') else None,
            compensation_fund_fee_vv=r.get('compensation_fund_fee_vv'),
            compensation_fund_fee_odo=r.get('compensation_fund_fee_odo'),
            compensation_fund_fee_odopayment_date=datetime.strptime(
                r['right']['compensation_fund_fee_odopayment_date'], '%Y-%m-%dT%X%z'
            ).strftime(f'{self.date_format} %X') if r.get('right') else None,
        )
