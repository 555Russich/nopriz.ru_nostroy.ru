import logging
from typing import Self, Type
from types import TracebackType

import gspread_asyncio as ga
from google.oauth2.service_account import Credentials

from config import FILEPATH_SERVICE_ACCOUNT
from src.schemas import NostroyRow, NoprizRow


def get_creds():
    creds = Credentials.from_service_account_file(FILEPATH_SERVICE_ACCOUNT)
    scoped = creds.with_scopes([
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    return scoped


agcm = ga.AsyncioGspreadClientManager(get_creds)


class GoogleSheets:
    def __init__(self):
        self.agc = None
        self.spreadsheet: ga.AsyncioGspreadSpreadsheet | None = None
        self.worksheets: list[ga.AsyncioGspreadWorksheet] | None = None
        self.worksheet: ga.AsyncioGspreadWorksheet | None = None
        self.worksheet_data: list[dict] | None = None

    async def __aenter__(self) -> Self:
        self.agc = await agcm.authorize()
        return self

    async def __aexit__(
            self,
            exc_type: Type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: TracebackType | None,
    ) -> None:
        pass

    async def open_spreadsheet(self, url: str) -> None:
        self.agc = await agcm.authorize()
        logging.info(f'Opening spreadsheet {url=}')
        self.spreadsheet = await self.agc.open_by_url(url)
        self.worksheets = await self.spreadsheet.worksheets()

    async def get_or_add_worksheet(self, sheet_index: int, name: str) -> None:
        if sheet_index > len(self.worksheets) - 1:
            self.worksheet = await self.spreadsheet.add_worksheet(
                title=name,
                rows=1000,
                cols=50,
                index=sheet_index
            )
            self.worksheets.append(self.worksheet)
        else:
            self.worksheet = await self.spreadsheet.get_worksheet(sheet_index)

    async def append(self, data: list[dict], append_columns: bool = False) -> None:
        self.agc = await agcm.authorize()

        if append_columns:
            columns = list(data[0].keys())
            await self.worksheet.append_row(columns)

        rows = [list(d.values()) for d in data]
        await self.worksheet.append_rows(rows, value_input_option='RAW')

    async def fill_new_rows(
            self, data: list[NoprizRow | NostroyRow],
            row_model: Type[NoprizRow] | Type[NostroyRow]
    ) -> None:
        ws_data = await self.worksheet.get_all_records(default_blank=None, value_render_option='FORMULA')
        ws_data_ids = [row_model(**row).id for row in ws_data]
        new_data = [row.dict(by_alias=True) for row in data if row.id not in ws_data_ids]
        logging.info(f'New rows after comparing: {len(new_data)}')

        if new_data:
            await self.append(new_data, append_columns=ws_data == [])
