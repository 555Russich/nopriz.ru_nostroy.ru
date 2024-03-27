from typing import Any

from pydantic import (
    BaseModel,
    Field,
    ConfigDict,
    model_validator,
    field_validator
)


class BaseRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True, coerce_numbers_to_str=True)

    id: int
    registration_number: str | None = Field(alias='Регистрационный номер члена СРО', default=None)
    sro: str | None = Field(alias='СРО', default=None)
    full_description: str | None = Field(alias='Полное наименование', default=None)
    short_description: str | None = Field(alias='Сокращенное наименование', default=None)
    ogrnip: int | None = Field(alias='ОГРН/ОГРНИП', default=None)
    inn: int | None = Field(alias='ИНН', default=None)
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
    member_status: int | None = None  # 2 - Исключен
    full_description: str | list[str] | None = None
    inn: int | list[int] | None = None
    ogrnip: int | list[int] | None = None
    registry_registration_date: str | list[str] | None = None  # 2024-03-13
