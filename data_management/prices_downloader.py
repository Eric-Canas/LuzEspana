"""
This class downloads the xls files that contain the prices of the day
"""

import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from data_management.constants import DOWNLOAD_PRICE_DAY_URL_XLS, DATA_FOLDER, EXPECTED_DATE_FORMAT, \
    SHEET_NAMES_TO_FOLDER
from urllib import request
import shutil
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
from loguru import logger
from threading import Lock


class PricesDownloader:
    def __init__(self, ):
        self.url = DOWNLOAD_PRICE_DAY_URL_XLS
        # lock for thread safety
        self.lock = Lock()

    def download_day(self, date: datetime) -> str:
        """
        Download the XLS from the URL and save it in filename

        :param date: Date of the prices to download. Only one day can be downloaded at a time
        :return: str. Path to the downloaded file

        :raises AssertionError: If the file is not downloaded, or if the date is not a datetime.datetime object
        """

        assert isinstance(date, datetime), f"date must be a datetime.datetime object, not {type(date)}"
        with self.lock:
            if not os.path.isdir(DATA_FOLDER):
                logger.warning(f"Folder {DATA_FOLDER} does not exist. Creating it")
                os.mkdir(DATA_FOLDER)

        date_str = date.strftime(EXPECTED_DATE_FORMAT)

        full_url = self.url.format(date=date_str)

        with request.urlopen(full_url) as response, NamedTemporaryFile(suffix='.xls', delete=False) as out_file:
            shutil.copyfileobj(fsrc=response, fdst=out_file)
            paths = self.cast_to_csv(out_file.name)
        os.remove(out_file.name)
        # Delete the temporary file
        assert not os.path.isfile(out_file.name), f"File {out_file.name} was not deleted"
        return paths

    def download_prices_for_date_range(self, start_date: datetime, end_date: datetime,
                                       _batch_size: int = 8) -> tuple[str, ...]:
        """
        Download the prices for a range of dates

        :param start_date: datetime.datetime object. Start date of the range
        :param end_date: datetime.datetime object. End date of the range

        :return: None
        """
        assert isinstance(start_date, datetime), f"start_date must be a datetime.datetime object, not {type(start_date)}"
        assert isinstance(end_date, datetime), f"end_date must be a datetime.datetime object, not {type(end_date)}"
        assert start_date < end_date, f"start_date must be before end_date"

        # Use ThreadPoolExecutor to download the files in parallel
        with ThreadPoolExecutor() as executor:
            files = []
            for i in tqdm(range(0, (end_date - start_date).days + 1, _batch_size), desc="Downloading prices"):
                batch_files = list(executor.map(self.download_day, [start_date + timedelta(days=i + j) for j in range(_batch_size)]))
                # Flatten the list
                batch_files = [file for sublist in batch_files for file in sublist]
                files.extend(batch_files)
        return tuple(files)


    def cast_to_csv(self, xls_path: str) -> list[str]:
        """
        Cast the xls file downloaded to a csv file that only contains the relevant columns

        :param xls_path: str. Path to the xls file downloaded
        :return: list[str]. List of paths to the csv files created
        """
        assert os.path.isfile(xls_path), f"File {xls_path} does not exist"
        file_dirs = {}
        for sheet_name, folder_path in SHEET_NAMES_TO_FOLDER.items():
            if not os.path.isdir(folder_path):
                logger.warning(f"Folder {folder_path} does not exist. Creating it")
                os.mkdir(folder_path)
            try:
                data = pd.read_excel(xls_path, sheet_name=sheet_name)
            except ValueError as e:
                # Old versions came this way
                sheet_name = "Tabla de Datos"
                data = pd.read_excel(xls_path, sheet_name=sheet_name)

            # Clean data because it comes as comes
            data = data.dropna(axis=0, how='all').dropna(axis=1, how='all')
            data = data.iloc[3:]  # 3 First rows are just headers
            # Keep only the relevant columns (0, 1, 2, 3, 4, 5 & 6)
            columns_names = ['date', 'hour', 'toll', 'period', 'PVPC_price_kwh', 'TEU_charges_kwh', 'TCU_production_price_kwh']
            data = data.iloc[:, :len(columns_names)]
            data.columns = columns_names
            # Substract one hour to the hour column (from 1:00 to 24:00 to 0:00 to 23:00)
            data['hour'] = data['hour'] - 1
            # Just in case sort by Hour
            data = data.sort_values(by='hour')
            # Cast 3 last columns from MGh to kWh (1 MWh = 1000 kWh)
            data.iloc[:, -3:] = data.iloc[:, -3:] / 1000

            # Format date from 2020-01-01 00:00:00 to 2020-01-01
            data['date'] = data['date'].apply(lambda x: x.strftime(EXPECTED_DATE_FORMAT))

            # Save one file per toll
            for toll in data['toll'].unique():
                data_toll = data[data['toll'] == toll]
                csv_path = os.path.join(folder_path, toll, f"{data['date'].iloc[0]}.csv")
                with self.lock:
                    if not os.path.isdir(os.path.dirname(csv_path)):
                        logger.warning(f"Folder {os.path.dirname(csv_path)} does not exist. Creating it")
                        os.makedirs(os.path.dirname(csv_path))
                # Just in case sort by Hour
                data_toll = data_toll.sort_values(by='hour')
                data_toll.to_csv(csv_path, index=False)
                file_dirs[sheet_name] = csv_path
                assert os.path.isfile(csv_path), f"File {csv_path} does not exist"

        return list(file_dirs.values())


