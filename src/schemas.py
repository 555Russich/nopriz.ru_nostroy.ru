from typing import Any

from pydantic import (
    BaseModel,
    Field,
    ConfigDict,
    model_validator,
)


class BaseRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True, coerce_numbers_to_str=True)

    id: int
    registration_number: str | None = Field(alias='Регистрационный номер члена СРО', default=None)
    sro: str | None = Field(alias='СРО', default=None)
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
    registry_registration_date: str | None = Field(alias='Дата регистрации в реестре', default=None)
    last_updated_at: str | None = Field(alias='Дата изменения информации', default=None)

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
    right_status: str | None = Field(alias='Статус Права', default=None)
    right_basis: str | None = Field(alias='Основание наделения правом', default=None)
    is_simple: bool | None = Field(alias='В отношении объектов капитального строительства (кроме особо опасных, технически сложных и уникальных объектов, объектов использования атомной энергии)', default=None)
    is_extremely_dangerous: bool | None = Field(alias='В отношении особо опасных, технически сложных и уникальных объектов капитального строительства (кроме объектов использования атомной энергии)', default=None)
    is_nuclear: bool | None = Field(alias='В отношении объектов использования атомной энергии', default=None)
    is_odo: bool | None = Field(alias='Сведения об ограничении права принимать участие в заключении договоров строительного подряда, договоров подряда на осуществление сноса объектов капитального строительства с использованием конкурентных способов заключения договоров', default=None)
    responsibility_level_odo: str | None = Field(alias='Размер обязательств по договорам подряда с использованием конкурентных способов заключения договоров (уровень ответственности)', default=None)
    responsibility_level_vv: str | None = Field(alias='Стоимость работ по одному договору подряда (уровень ответственности)', default=None)
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
    member_right_odo: float | None = Field(
        alias='Размер взноса в компенсационный фонд обеспечения договорных обязательств', default=None)


class FiltersNostroy(BaseModel):
    region_number: int | list[int] | None = None
    sro_registration_number: str | list[str] | None = None
    director: str | list[str] | None = None
    sro_full_description: str | list[str] | None = None
    member_status: int | None = None  # 1 - Является членом, 2 - Исключен
    sro_enabled: bool | None = None
    full_description: str | list[str] | None = None
    inn: int | list[int] | None = None
    ogrnip: int | list[int] | None = None
    registry_registration_date: str | list[str] | None = None  # 2024-03-13
