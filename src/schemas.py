from typing import Any, Self

from pydantic import (
    BaseModel,
    Field,
    ConfigDict,
    model_validator,
)


class MyBaseModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, coerce_numbers_to_str=True)


class SRO(MyBaseModel):
    full_description: str | None = Field(alias='Полное название СРО', default=None)
    short_description: str | None = Field(alias='Сокращенное название СРО', default=None)
    registration_number: str | None = Field(alias='Регистрационный номер в государственном реестре саморегулируемых организаций', default=None)
    inn: str | None = Field(alias='ИНН', default=None)
    ogrn: str | None = Field(alias='ОГРН', default=None)
    place: str | None = Field(alias='Адрес места нахождения', default=None)
    phone: str | None = Field(alias="Контактный телефон", default=None)
    email: str | None = Field(alias='Адрес электронной почты', default=None)
    site: str | None = Field(alias='Адрес официального сайта в информационно-телекоммуникационной сети "Интернет"', default=None)

    @property
    def as_string(self) -> str:
        paragraphs = [
            self.full_description,
            self.short_description,
            self.registration_number,
            f'ИНН:{self.inn}',
            f'ОГРН:{self.ogrn}',
            f'Адрес местонахождения:{self.place}',
            f'Телефон:{self.phone}',
            f'Email:{self.email}',
            f'Адрес сайта: {self.site}'
        ]
        return ';\n'.join([p for p in paragraphs if p])


class Check(MyBaseModel):
    check_date: str | None = Field(alias='Дата начала проверки', default=None)
    disciplinary_action: str | None = Field(alias='Факты применения мер дисциплинарного воздействия', default=None)
    member_check_result: str | None = Field(alias='Результат проверки члена СРО', default=None)
    member_check_type: str | None = Field(alias='Тип проверки', default=None)


class Insurance(MyBaseModel):
    begin_date: str | None = Field(alias='Начало действия договора', default=None)
    contract_number: str | None = Field(alias='Номер договора', default=None)
    end_date: str | None = Field(alias='Окончание действия договора', default=None)
    insurance_sum: str | None = Field(alias='Размер страховой суммы', default=None)
    insurer: str | None = Field(alias='Наименование страховой компании', default=None)
    license: str | None = Field(alias='Лицензия', default=None)
    object_title: str | None = Field(alias='Предмет договора страхования', default=None)
    phone: str | None = Field(alias='Контактные телефоны', default=None)
    place: str | None = Field(alias='Место нахождения', default=None)


class BaseRow(MyBaseModel):
    id: int
    sro: SRO
    registration_number: str | None = Field(alias='Регистрационный номер члена СРО', default=None)
    full_description: str | None = Field(alias='Полное наименование', default=None)
    short_description: str | None = Field(alias='Сокращенное наименование', default=None)
    ogrnip: int | str | None = Field(alias='ОГРН/ОГРНИП', default=None)
    inn: int | str | None = Field(alias='ИНН', default=None)
    phones: str | None = Field(alias='Номер контактного телефона', default=None)
    member_type: str | None = Field(alias='Тип члена СРО', default=None)
    full_address: str | None = Field(alias='Адрес места нахождения юридического лица', default=None)
    director: str | None = Field(
        alias='ФИО, осуществляющего функции единоличного исполнительного органа юридического '
              'лица и (или) руководителя коллегиального исполнительного органа юридического лица', default=None)
    accordance_status: str | None = Field(
        alias='Сведения о соответствии члена СРО условиям членства в СРО, предусмотренным '
              'законодательством РФ и (или) внутренними документами СРО', default=None)
    other_information: str | None = Field(alias='Иные сведения, предусмотренные требованиями СРО', default=None)
    members_total_liability: str | None = Field(alias='Фактический совокупный размер обязательств члена саморегулируемой организации по договорам подряда',default=None)
    lialbility_date: str | None = Field(alias='Дата обновления', default=None)
    registry_registration_date: str | None = Field(alias='Дата регистрации в реестре', default=None)
    last_updated_at: str | None = Field(alias='Дата изменения информации', default=None)
    right_status: str | None = Field(alias='Статус Права', default=None)
    right_status_vv: str | None = Field(alias='Статус права ВВ', default=None)
    right_status_odo: str | None = Field(alias='Статус права ОДО', default=None)
    odo_compensation_fund_date: str | None = Field(alias='Дата оплаты ОДО', default=None)
    odo_responsibility_level_date: str | None = Field(alias='Дата доп. взноса ОДО', default=None)
    right_basis: str | None = Field(alias='Основание наделения правом', default=None)
    is_simple: str | None = Field(alias='В отношении объектов капитального строительства (кроме особо опасных, технически сложных и уникальных объектов, объектов использования атомной энергии)', default=None)
    is_extremely_dangerous: str | None = Field(alias='В отношении особо опасных, технически сложных и уникальных объектов капитального строительства (кроме объектов использования атомной энергии)', default=None)
    is_nuclear: str | None = Field(alias='В отношении объектов использования атомной энергии', default=None)
    responsibility_level_odo: str | None = Field(alias='Размер обязательств по договорам подряда с использованием конкурентных способов заключения договоров (уровень ответственности)', default=None)
    responsibility_level_vv: str | None = Field(alias='Стоимость работ по одному договору подряда (уровень ответственности)', default=None)

    @staticmethod
    def parse_full_address(data: dict[str, ...]) -> str:
        address_info = [
            data.get('index'),
            data.get('country'),
            data.get('subject'),
            data.get('district'),
            data.get('locality'),
            data.get('street'),
            data.get('house'),
            data.get('building'),
            data.get('room')
        ]
        return ', '.join([info for info in address_info if info is not None])

    @model_validator(mode='before')
    @classmethod
    def serialize_model(cls, d: Any) -> Any:
        if isinstance(d, dict):
            for k, v in d.items():
                if v is None:
                    continue
                elif v == '':
                    d[k] = None
        return d


class NostroyRow(BaseRow):
    region: str | None = Field(alias='Регион', default=None)
    is_odo: bool | None = Field(alias='Сведения об ограничении права принимать участие в заключении договоров строительного подряда, договоров подряда на осуществление сноса объектов капитального строительства с использованием конкурентных способов заключения договоров', default=None)
    compensation_fund_fee_odo: str | None = Field(alias='Размер взноса в компенсационный фонд обеспечения договорных обязательств', default=None)
    compensation_fund_fee_vv: str | None = Field(alias='Размер взноса в компенсационный фонд возмещения вреда', default=None)
    compensation_fund_fee_odopayment_date: str | None = Field(alias=' Дата уплаты взноса (дополнительного взноса)', default=None)


class NoprizRow(BaseRow):
    member_status: str | None = Field(alias='Статус члена СРО', default=None)
    basis: str | None = Field(alias='Дата и номер решения о приеме в члены', default=None)
    approved_basis_date: str | None = Field(alias='Дата вступления в силу решения о приеме', default=None)
    created_at: str | None = Field(alias='Дата создания', default=None)
    suspension_date: str | None = Field(alias='Дата прекращения членства', default=None)
    suspension_reason: str | None = Field(alias='Основание прекращения членства', default=None)
    member_right_vv: float | None = Field(alias='Размер взноса в компенсационный фонд возмещения вреда', default=None)
    member_right_odo: float | None = Field(alias='Размер взноса в компенсационный фонд обеспечения договорных обязательств', default=None)
    simple_date: str | None = Field(alias='Дата (В отношении объектов капитального строительства (кроме особо опасных, технически сложных и уникальных объектов, объектов использования атомной энергии))', default=None)
    extremely_dangerous_date: str | None = Field(alias='Дата (В отношении особо опасных, технически сложных и уникальных объектов капитального строительства (кроме объектов использования атомной энергии))', default=None)
    nuclear_date: str | None = Field(alias='Дата (В отношении объектов использования атомной энергии:)', default=None)
    checks: list[Check] | None
    insurances: list[Insurance] | None


class BaseFilters(MyBaseModel):
    member_status: int | None = None  # 1 - Является членом, 2 - Исключен
    sro_enabled: bool | None = None
    sro_registration_number: str | list[str] | None = None


class FiltersNostroy(BaseFilters):
    region_number: int | list[int] | None = None
    director: str | list[str] | None = None
    sro_full_description: str | list[str] | None = None
    full_description: str | list[str] | None = None
    inn: int | list[int] | None = None
    ogrnip: int | list[int] | None = None
    registry_registration_date: str | list[str] | None = None  # 2024-03-13


class FiltersNopriz(BaseFilters):
    pass
