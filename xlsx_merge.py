import asyncio
import logging
from datetime import datetime
from pathlib import Path
from io import BytesIO
from copy import copy
import tqdm

import aiofiles
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.worksheet import Worksheet

from config import DATE_FORMAT
from src.my_logging import get_logger
from src.scrappers.nopriz import ScraperNopriz
from src.schemas import FiltersNostroy, FiltersNopriz

DATE_FROM = '01.01.1900'
# DATE_FROM = '13.06.2024'
DATE_TO = datetime.now().date().strftime(DATE_FORMAT)


def append_to_excel(ws_main: Worksheet, xlsx_bytes: bytes) -> None:

    wb_cart = load_workbook(BytesIO(xlsx_bytes))
    ws_cart = wb_cart.worksheets[0]
    last_row = ws_main.max_row - 1 if ws_main.max_row == 1 else ws_main.max_row - 3
    row_slice = 2 if ws_main.max_row > 2 else 0

    for i, row in enumerate(ws_cart.rows):
        if i >= row_slice:
            for cell in row:
                new_row = cell.row

                if cell.column >= 2:
                    if cell.row == 3:
                        continue
                    elif cell.row > 3:
                        new_row = cell.row - 1

                try:
                    new_cell = ws_main.cell(row=last_row + new_row, column=cell.column, value=cell.value)
                except AttributeError:
                    new_cell = ws_main.cell(row=last_row + new_row, column=cell.column, value=None)
                    # print(row)
                    # for l in ws_cart.values:
                    #     print(l)
                    # raise

                if cell.has_style:
                    new_cell.font = copy(cell.font)
                    # if cell.column != 1 and cell.row != ws_cart.max_row:
                    new_cell.border = copy(cell.border)
                    # new_cell.fill = copy(cell.fill)
                    new_cell.number_format = copy(cell.number_format)
                    # new_cell.protection = copy(cell.protection)
                    new_cell.alignment = copy(cell.alignment)

    for mc in ws_cart.merged_cells.ranges:
        if mc.min_row >= row_slice:
            min_row = mc.min_row
            max_row = mc.max_row

            if mc.min_row == 3 and mc.min_col == 1:
                max_row = mc.max_row - 1
            elif mc.min_row >= 4 and mc.min_col >= 2:
                min_row = mc.min_row - 1
                max_row = mc.max_row - 1

            ws_main.merge_cells(
                start_row=last_row + min_row,
                start_column=mc.min_col,
                end_row=last_row + max_row,
                end_column=mc.max_col
            )


async def main():
    date_from = datetime.strptime(DATE_FROM, DATE_FORMAT)
    date_to = datetime.strptime(DATE_TO, DATE_FORMAT)
    proxy_url = ''

    async with ScraperNopriz(date_format=DATE_FORMAT, date_from=date_from, date_to=date_to) as scrapper:
        filters = FiltersNopriz(member_status=1, sro_enabled=True, sro_registration_number='И')
        ids = await scrapper.get_ids(filters=filters, use_cached=True)
        ids = ids[:10]

        filepath_main = Path(scrapper.get_filename('nopriz')).with_suffix('.xlsx')

        ws_main = wb_main.worksheets[0]
        logging.info(f'Red workbook')

        tasks_download = []
        for n, id_ in enumerate(ids):
            tasks_download.append(scrapper.download_xlsx_cart(id_))

            if len(tasks_download) == 20 or id_ == ids[-1]:
                results = await asyncio.gather(*tasks_download)
                logging.info(f'Downloaded bunch of files')

                # tasks_append_to_xlsx = [append_to_excel(ws_main=ws_main, xlsx_bytes=r) for r in results]
                # await asyncio.gather(*tasks_append_to_xlsx)
                for xlsx_bytes in results:
                    append_to_excel(ws_main=ws_main, xlsx_bytes=xlsx_bytes)

                tasks_download = []
                logging.info(f'{n}/{len(ids)} Appended data to worksheet')

            if n % 100 == 0:
                logging.info('saving...file')
                wb_main.save(filepath_main)
        wb_main.save(filepath_main)


if __name__ == '__main__':
    get_logger(Path(__file__).stem + '.log')

    try:
        asyncio.run(main())
    except Exception as ex:
        logging.error(ex, exc_info=True)
        exit(1)
