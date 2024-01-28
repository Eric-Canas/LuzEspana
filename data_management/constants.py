import os

DOWNLOAD_PRICE_DAY_URL_XLS = 'https://api.esios.ree.es/archives/71/download?date={date}'
EXPECTED_DATE_FORMAT = '%Y-%m-%d'

DATA_FOLDER = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
TEMP_FOLDER = os.path.join(DATA_FOLDER, 'temp')


SHEET_NAMES_TO_FOLDER = {
    'Tabla de Datos PCB': os.path.join(DATA_FOLDER, 'PCB'),
    'Tabla de Datos CYM': os.path.join(DATA_FOLDER, 'CYM'),
}