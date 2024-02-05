import os
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, date, timedelta

from google.cloud.firestore_v1 import FieldFilter
from loguru import logger
import firebase_admin
from firebase_admin import credentials as firebase_crendentials, firestore
from tqdm import tqdm
from data_management.constants import DATA_FOLDER, BY_DAY, NO_AGGREGATION
from utils.utils import load_csv_as_dicts, get_all_files_with_filename_in_subfolders, get_doc_id_for_row, \
    get_collection_name, get_collection

CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "resources", "credentials",
                                "electric-bill-backtesting-firebase-adminsdk-d44q1-c89a4a1bb7.json")

class FirebaseManager:
    def __init__(self):
        credentials = firebase_crendentials.Certificate(cert=CREDENTIALS_PATH)
        firebase_admin.initialize_app(credential=credentials)
        self.client = firestore.client()


    def post_day(self, day: date, skip_if_exist: bool = True) -> bool:
        """
        Post the content of a csv file to the firebase database

        :param day: date. Day of the data to post (example: datetime(year=2021, month=6, day=1))
        """
        date_str = day.strftime("%Y-%m-%d")
        files = get_all_files_with_filename_in_subfolders(parent_folder=DATA_FOLDER,
                                                          filename=f"{date_str}.csv")
        assert len(files) > 0, f"No files found for date {date_str}"
        # Files will have the format {<PCB/CYM>: {<TOLL>: <PATH>}}
        for location, tolls in files.items():
            for toll, file_path in tolls.items():
                if skip_if_exist and self.data_exists(day=day, location=location, toll=toll):
                    logger.info(f"Data for {date_str} already exists in the database. Skipping")
                    continue
                rows = load_csv_as_dicts(csv_path=file_path)
                # Post the data to the database
                self.__post(rows=rows, location=location, toll=toll)

        return True

    def __post(self, rows: list[dict[str, str | datetime | float | int]], location: str, toll: str) -> bool:
        """
        Post the data to the database
        :param rows: list[dict[str, str | datetime | float | int]]. Data to post, should be always 24 rows, one for each hour
        :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
        :param toll: str. Toll of the data (2.0TD, 2.0A, 2.0DHA, 2.0-DHS...)

        :return: bool. True if the data was posted successfully, False otherwise
        """
        # It happens during the day that the summer time changes, so there are 23 rows instead of 24
        if len(rows) == 23:
            fake_last_hour = deepcopy(rows[-1])
            fake_last_hour['hour'] = 23
            rows.append(fake_last_hour)
        # It happens during the day that the winter time changes, let's assume this last hour never existed
        elif len(rows) == 25:
            rows = rows[:-1]
        assert len(rows) == 24, f"Expected 24 rows, got {len(rows)}"

        ok_no_aggregation = self.__post_no_aggregation(rows=rows, location=location, toll=toll)
        ok_day_aggregation = self.__post_day_aggregation(rows=rows, location=location, toll=toll)

        return ok_no_aggregation and ok_day_aggregation

    def __post_no_aggregation(self, rows: list[dict[str, str | datetime | float | int]], location: str, toll: str) -> bool:
        """
        Post the data to the database
        :param rows: list[dict[str, str | datetime | float | int]]. Data to post, should be always 24 rows, one for each hour
        :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
        :param toll: str. Toll of the data (2.0TD, 2.0A, 2.0DHA, 2.0-DHS...)

        :return: bool. True if the data was posted successfully, False otherwise
        """

        assert len(rows) == 24, f"Expected 24 rows, got {len(rows)}"

        collection_ref = get_collection(client=self.client, location=location, toll=toll, aggregation=NO_AGGREGATION)

        # Iterate over each row and post it to the database
        for row in rows:
            # Format datetime_spain to create the document_id
            doc_id = get_doc_id_for_row(row=row)
            # Create a document reference
            doc_ref = collection_ref.document(doc_id)
            # Post the data to the database
            doc_ref.set(row)

        return True

    def __post_day_aggregation(self, rows: list[dict[str, str | datetime | float | int]], location: str, toll: str) -> bool:
        """
        Post the data to the database
        :param rows: list[dict[str, str | datetime | float | int]]. Data to post, should be always 24 rows, one for each hour
        :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
        :param toll: str. Toll of the data (2.0TD, 2.0A, 2.0DHA, 2.0-DHS...)

        :return: bool. True if the data was posted successfully, False otherwise
        """
        # It happens during the day that the summer time changes, so there are 23 rows instead of 24
        assert len(rows) == 24, f"Expected 24 rows, got {len(rows)}"

        collection_ref = get_collection(client=self.client, location=location, toll=toll, aggregation=BY_DAY)
        full_day_row = {
            'datetime_spain': rows[0]['datetime_spain'],
            'date': rows[0]['date'],
            'location': location,
            'toll': toll,

            'PVPC_price_kwh': [row['PVPC_price_kwh'] for row in rows],
            'TEU_charges_kwh': [row['TEU_charges_kwh'] for row in rows],
            'TCU_production_price_kwh': [row['TCU_production_price_kwh'] for row in rows],
            'period': [row['period'] for row in rows]
        }

        assert all(key in rows[0] for key in full_day_row.keys()), f"Some defined names mismatch with NO_AGGREGATION"

        # Format datetime_spain to create the document_id
        doc_id = get_doc_id_for_row(row=full_day_row)
        # Create a document reference
        doc_ref = collection_ref.document(doc_id)
        # Post the data to the database
        doc_ref.set(full_day_row)
        return True


    def data_exists(self, day: date, location: str = 'PCB', toll: str = '2.0TD') -> bool:
        """
        Check if the data for a given day from the database, determining a location and a toll.
        :param day: date. Day of the data to get (example: datetime(year=2021, month=6, day=1))
        :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
        :param toll: str. Toll of the data (2.0TD, 2.0A, 2.0.DHA, 2.0.DHS...)

        :return: bool. True if the data exists, False otherwise
        """
        # Reference to the collection
        doc_id = get_doc_id_for_row(row={'datetime_spain': day, 'location': location, 'toll': toll})
        collection_ref_no_aggregation = get_collection(client=self.client, location=location, toll=toll, aggregation=NO_AGGREGATION)
        collection_ref_day_aggregation = get_collection(client=self.client, location=location, toll=toll, aggregation=BY_DAY)
        # Get the document
        doc_no_aggregation = collection_ref_no_aggregation.document(doc_id).get()
        doc_day_aggregation = collection_ref_day_aggregation.document(doc_id).get()
        # Get True, only if it exists in both collections (to avoid partial data)
        return doc_no_aggregation.exists and doc_day_aggregation.exists


    def post_for_date_range(self, start_date: date, end_date: date, _batch_size: int = 8) -> bool:
        """
        Post the content of a csv file to the firebase database

        :param start_date: date. Start date of the range
        :param end_date: date. End date of the range
        """
        assert start_date < end_date, f"start_date must be before end_date"
        for i in tqdm(range(0, (end_date - start_date).days + 1, _batch_size), desc="Posting data"):
            with ThreadPoolExecutor() as executor:
                batch_size = min(_batch_size, (end_date - start_date).days - i + 1)
                posted = list(executor.map(self.post_day, [start_date + timedelta(days=i + j) for j in range(batch_size)]))
                assert all(posted), f"Not all data was posted successfully"

        return True

    def delete_collection(self, coll_ref, batch_size):
        docs = coll_ref.list_documents(page_size=batch_size)
        deleted = 0

        for doc in docs:
            print(f"Deleting doc {doc.id} => {doc.get().to_dict()}")
            doc.delete()
            deleted = deleted + 1

        if deleted >= batch_size:
            return self.delete_collection(coll_ref, batch_size)


    def __del__(self):
        # Close the connection to the database
        self.client.close()
