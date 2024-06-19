from pathlib import Path

DIR_SECRETS = Path('secrets')
DIR_XLSX = Path('xlsx')
DIR_CARTS = DIR_XLSX / 'carts'

FILEPATH_SERVICE_ACCOUNT = DIR_SECRETS / 'service_account.json'
FILEPATH_CERT = DIR_SECRETS / 'globalsign.cer'

DATE_FORMAT = '%d.%m.%Y'
# URL_SPREADSHEET = 'https://docs.google.com/spreadsheets/d/1PGQ2ITMpRz9i9RzQSELT8GFexNun8fviHgUQwU57SxE/edit#gid=0'
URL_SPREADSHEET = 'https://docs.google.com/spreadsheets/d/1PGQ2ITMpRz9i9RzQSELT8GFexNun8fviHgUQwU57SxE/edit#gid=0'
