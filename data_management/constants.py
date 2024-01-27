import os

DOWNLOAD_PRICE_DAY_URL_XLS = 'https://api.esios.ree.es/archives/71/download?date={date}'
EXPECTED_DATE_FORMAT = '%Y-%m-%d'

DATA_FOLDER = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')