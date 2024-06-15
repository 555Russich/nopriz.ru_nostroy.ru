import logging
import re
from datetime import datetime
from pathlib import Path
from openpyxl import load_workbook, Workbook

from src.scrappers.base import BaseScrapper
from src.schemas import NoprizRow, SRO


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
            r_json = await self.request_json(
                    method='POST',
                    url='https://reestr.nopriz.ru/api/sro/all/member/list',
                    headers=self.headers,
                    proxy=self.proxy_url,
                    json=json_
            )

            for d in r_json['data']['data']:
                dt = datetime.strptime(
                    re.sub(r'\+0\d:00', '', d['registry_registration_date']),
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

    async def get_sro_member(self, id_: int) -> NoprizRow:
        r = await self.request_json(
            method='POST',
            url=f'https://reestr.nopriz.ru/api/member/{id_}/info',
            headers=self.headers,
            proxy=self.proxy_url
        )
        r = r['data']

        sro_id = int(r['sro']['id'])
        if sro_id in self.sro_cache:
            sro = self.sro_cache[sro_id]
        else:
            sro = await self.get_sro(sro_id)
            self.sro_cache[id_] = sro

        try:
            return NoprizRow(
                id=r['id'],
                sro=sro,
                registration_number=r['registration_number'],
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
                member_right_odo=r['member_right_odo'].get('compensation_fund') if r.get('member_right_odo') else None,
                right_status=r['right']['right_status']['title'] if r.get('right') else None,
                right_basis=r['right']['basis'] if r.get('right') else None,
                is_simple=r['member_right_vv']['is_simple'] if r.get('member_right_vv') else False,
                simple_date=datetime.strptime(
                    r['member_right_vv']['simple_date'], '%Y-%m-%dT%X%z'
                ).strftime(self.date_format)
                if r.get('member_right_vv') and r['member_right_vv'].get('simple_date') else None,
                is_extremely_dangerous=r['member_right_vv']['is_extremely_dangerous'] if r.get('member_right_vv') else False,
                extremely_dangerous_date=datetime.strptime(
                    r['member_right_vv']['extremely_dangerous_date'], '%Y-%m-%dT%X%z'
                ).strftime(self.date_format)
                if r.get('member_right_vv') and r['member_right_vv'].get('extremely_dangerous_date') else None,
                is_nuclear=r['member_right_vv']['is_nuclear']
                if r.get('member_right_vv') else False,
                nuclear_date=datetime.strptime(
                    r['member_right_vv']['nuclear_date'], '%Y-%m-%dT%X%z'
                ).strftime(self.date_format)
                if r.get('member_right_vv') and r['member_right_vv'].get('nuclear_date') else None,
                responsibility_level_odo=f'{r['member_right_odo']['responsibility_level']['title']}, '
                                         f'{r['member_right_odo']['responsibility_level']['cost']}'
                if r.get('member_right_odo') and r['member_right_odo'].get('responsibility_level') else None,
                responsibility_level_vv=f'{r['member_right_vv']['responsibility_level']['title']}, '
                                        f'{r['member_right_vv']['responsibility_level']['cost']}'
                if r.get('member_right_vv') and r['member_right_vv'].get('responsibility_level') else None,
            )
        except Exception as ex:
            logging.info(f'{id_=}\n{r}')
            raise ex

    async def get_sro(self, id_: int) -> SRO:
        r = await self.request_json(url=f'https://reestr.nopriz.ru/api/sro/{id_}')
        r = r['data']
        return SRO(**r)

    @classmethod
    def to_excel(cls, filepath: Path, data: list[NoprizRow]) -> None:
        if filepath.exists():
            wb = load_workbook(filepath)
            ws = wb.worksheets[0]
        else:
            wb = Workbook()
            ws = wb.worksheets[0]

            first_row = {
                'Сведения о СРО': 'A1',
                'Сведения о члене саморегулируемой организации': 'B1:S1',
                'Размер обязательств': 'T1:U1',
                'Сведения о КФ': 'V1:Y1',
                'Сведения о наличии права': 'Z1:AH1',
                'Сведения о проверках': 'AI1:AL1',
                'Сведения об обеспечении имущественной ответственности': 'AM1:AQ1'
            }
            for v, range_string in first_row.items():
                first_cell = range_string.split(':')[0]
                ws[first_cell] = v
                ws.merge_cells(range_string=range_string)

            second_row = [
                'Полное и (в случае, если имеется) сокращенное наименование саморегулируемой организации, основной государственный регистрационный номер записи о государственной регистрации юридического лица, идентификационный номер налогоплательщика, регистрационный номер в государственном реестре саморегулируемых организаций, адрес места нахождения, адрес официального сайта в информационно-телекоммуникационной сети "Интернет", адрес электронной почты, контактный телефон',
                'N п/п',
                'Полное наименование',
                'Сокращенное наименование',
                'Регистрационный номер в реестре СРО',
                'Дата регистрации в реестре СРО',
                # 'Дата государственной регистрации',
                'Статус (члена СРО)',
                'Дата прекращения членства',
                'Основание прекращения членства',
                'ОГРН/ОГРНИП',
                'ИНН',
                'Контактные телефоны',
                'Адрес места нахождения юридического лица',
                'Фамилия, имя, отчество лица, осуществляющего функции единоличного исполнительного органа юридического лица и (или) руководителя коллегиального исполнительного органа юридического лица',
                # 'Фамилия, имя, отчество (при наличии) для ИП',
                # 'Адрес места фактического осуществления деятельности для ИП',
                'Сведения о соответствии',
                'Иные сведения, предусмотренные требованиями СРО',
                'Фактический совокупный размер обязательств члена саморегулируемой организации по договорам подряда',
                'Дата обновления',
                'Размер взноса в компенсационный фонд возмещения вреда',
                'Размер взноса в компенсационный фонд обеспечения договорных обязательств',
                'Дата вступления в силу решения о приеме в члены',
                'Номер решения о приеме в члены, дата решения о приеме в члены',
                'В отношении объектов капитального строительства (кроме особо опасных, технически сложных и уникальных объектов, объектов использования атомной энергии)',
                'В отношении особо опасных, технически сложных и уникальных объектов капитального строительства (кроме объектов использования атомной энергии)',
                'В отношении объектов использования атомной энергии',
                'Стоимость работ по одному договору подряда (уровень ответственности)',
                'Статус права ВВ',
                'Размер обязательств по договорам подряда с использованием конкурентных способов заключения договоров (уровень ответственности)',
                'Статус права ОДО',
                'Дата оплаты ОДО',
                'Дата доп. взноса ОДО',
                'Дата окончания проверки',
                'Тип проверки',
                'Результат проверки члена СРО',
                'Факты применения мер дисциплинарного воздействия',
                'Предмет договора страхования',
                'Размер страховой суммы',
                'Наименование страховой компании',
                'Местонахождение',
                'Контактные телефоны'
            ]
            ws.append(second_row)

        for r in data:
            values = [
                r.sro.as_string,
                r.id,
                r.full_description,
                r.short_description,
                r.registration_number,
                r.registry_registration_date,
                r.member_status,
                r.suspension_date,
                r.suspension_reason,
                r.ogrnip,
                r.inn,
                r.full_address,
                r.director,
                r.accordance_status,
                r.other_information
            ]

        wb.save(filepath)
        logging.info(f'{filepath} was written')
