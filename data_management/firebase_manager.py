import os
from datetime import datetime, date

import firebase_admin
from firebase_admin import credentials as firebase_crendentials, firestore

from data_management.constants import DATA_FOLDER
from utils.utils import load_csv_as_dicts, get_all_files_with_filename_in_subfolders, get_doc_id_for_row

CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "..", "resources", "credentials",
                                "electric-bill-backtesting-firebase-adminsdk-d44q1-c89a4a1bb7.json")

class FirebaseManager:
    def __init__(self):
        credentials = firebase_crendentials.Certificate(cert=CREDENTIALS_PATH)
        firebase_admin.initialize_app(credential=credentials)
        self.client = firestore.client()


    def post_day_of_data(self, day: date, skip_if_exist: bool = True) -> bool:
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
                    continue
                rows = load_csv_as_dicts(csv_path=file_path)
                # Post the data to the database
                self.__post(rows=rows, location=location, toll=toll)
        # Read the CSV file
        print(f"Posting data for date {date_str}")
        return True

    def __post(self, rows: list[dict[str, str | datetime | float | int]], location: str, toll: str) -> bool:
        """
        Post the data to the database
        :param rows: list[dict[str, str | datetime | float | int]]. Data to post, should be always 24 rows, one for each hour
        :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
        :param toll: str. Toll of the data (2-0TD, 2-0A, 2-0-DHA, 2-0-DHS...)

        :return: bool. True if the data was posted successfully, False otherwise
        """
        assert len(rows) == 24, f"Expected 24 rows, got {len(rows)}"
        # Reference to the collection
        collection_ref = self.client.collection(location)

        # Iterate over each row and post it to the database
        for row in rows:
            # Format datetime_spain to create the document_id
            ddoc_id = get_doc_id_for_row(row=row, location=location)
            # Create a document reference
            doc_ref = collection_ref.document(doc_id)
            # Post the data to the database
            doc_ref.set(row)

        return True

    def get_data_for_day(self, day: date, location: str = 'PCB', toll: str = '2-0TD') -> \
            list[dict[str, str | datetime | float | int]]:
        """
        Get the data for a given day from the database, determining a location and a toll.
        :param day: date. Day of the data to get (example: datetime(year=2021, month=6, day=1))
        :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
        :param toll: str. Toll of the data (2-0TD, 2-0A, 2-0-DHA, 2-0-DHS...)

        :return: list[dict[str, str | datetime | float | int]]. List of dictionaries with the data for the given day
        """
        date_str = day.strftime("%Y-%m-%d")
        # Reference to the collection
        collection_ref = self.client.collection(location)
        # Get all the documents from the collection
        docs = collection_ref.where('date', '==', date_str).where('toll', '==', toll).stream()
        # Get the data from the documents
        data = [doc.to_dict() for doc in docs]
        return data

    def data_exists(self, day: date, location: str = 'PCB', toll: str = '2-0TD') -> bool:
        """
        Check if the data for a given day from the database, determining a location and a toll.
        :param day: date. Day of the data to get (example: datetime(year=2021, month=6, day=1))
        :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
        :param toll: str. Toll of the data (2-0TD, 2-0A, 2-0-DHA, 2-0-DHS...)

        :return: bool. True if the data exists, False otherwise
        """
        date_str = day.strftime("%Y-%m-%d")
        # Reference to the collection
        collection_ref = self.client.collection(location)
        # Get all the documents from the collection
        docs = collection_ref.where('date', '==', date_str).where('toll', '==', toll).stream()
        # Only need to check if there is at least one document
        first_doc = next(docs, None)
        if first_doc:
            return True
        else:
            return False