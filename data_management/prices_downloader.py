"""
This class downloads the xls files that contain the prices of the day
"""
import os

from data_management.constants import DOWNLOAD_PRICE_DAY_URL_XLS, DATA_FOLDER, EXPECTED_DATE_FORMAT
from urllib import request
import shutil
from datetime import datetime
from loguru import logger


class PricesDownloader:
    def __init__(self, ):
        self.url = DOWNLOAD_PRICE_DAY_URL_XLS

    def download_day(self, date: datetime) -> str:
        """
        Download the XLS from the URL and save it in filename

        :param date: Date of the prices to download. Only one day can be downloaded at a time
        :return: str. Path to the downloaded file

        :raises AssertionError: If the file is not downloaded, or if the date is not a datetime.datetime object
        """

        assert isinstance(date, datetime), f"date must be a datetime.datetime object, not {type(date)}"
        if not os.path.isdir(DATA_FOLDER):
            logger.warning(f"Folder {DATA_FOLDER} does not exist. Creating it")
            os.mkdir(DATA_FOLDER)

        date_str = date.strftime(EXPECTED_DATE_FORMAT)
        path = os.path.join(DATA_FOLDER, f"{date_str}.xls")
        full_url = self.url.format(date=date_str)

        with request.urlopen(full_url) as response, open(path, 'wb') as out_file:
            shutil.copyfileobj(fsrc=response, fdst=out_file)
        assert os.path.isfile(path), f"File {path} does not exist"

        return path