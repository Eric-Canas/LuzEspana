import os
from collections import defaultdict
from datetime import datetime
import pandas as pd

from data_management.constants import PVPC_PRICES, LOCATIONS, TOLLS, NO_AGGREGATION, PRICES, LOCATION_DESCRIPTIONS, \
    BY_DAY

CHECKED_LOCATIONS, CHECKED_TOLLS = {NO_AGGREGATION: set(), BY_DAY: set()}, {NO_AGGREGATION: defaultdict(set), BY_DAY: defaultdict(set)}

def load_csv_as_dicts(csv_path: str, datetime_column_name: str = 'datetime_spain') -> list[dict]:
    """
    Post the content of a csv file to the firebase database

    :param csv_path: str. Path to the csv file to post
    :return: list[dict]. List of dictionaries with the data from the csv file
    """
    # Read the csv
    df = pd.read_csv(filepath_or_buffer=csv_path, sep=',')
    # If the day contains 25 hours, that's a winter time change, let's assume the last hour never existed
    if len(df) == 25:
        df = df[:-1]
    # Build a date time column
    df[datetime_column_name] = df.apply(
        lambda row: datetime.strptime(f"{row['date']} {row['hour']}:00", "%Y-%m-%d %H:%M"), axis=1)

    # Convertir el dataframe a una lista de diccionarios
    data_list = df.to_dict(orient='records')

    return data_list

def get_all_files_with_filename_in_subfolders(parent_folder: str, filename: str) -> dict[dict[str, str]]:
    """
    Get all the files with a given filename in a folder and its subfolders. Organized by subfolders as
    a dict. For example: {'subfolder1': 'full_path/filename', 'subfolder2': {'subsubfolder1': 'full_path/filename'}}

    :param parent_folder: str. Path to the parent folder
    :param filename: str. Name of the file to find

    :return: dict[dict[str, str]]. Dict with the files found organized by subfolders.
    """
    assert os.path.isdir(parent_folder), f"Folder {parent_folder} does not exist"

    # Dictionary to hold the results
    files_dict = {}

    # Iterate through items in the parent_folder
    for item in os.listdir(parent_folder):
        full_path = os.path.join(parent_folder, item)
        if os.path.isdir(full_path):
            # If the item is a directory, recursively search in it
            files_in_subfolder = get_all_files_with_filename_in_subfolders(full_path, filename)
            if files_in_subfolder:
                files_dict[item] = files_in_subfolder
        elif os.path.isfile(full_path) and item == filename:
            # If the item is a file and matches the filename, add its path
            files_dict = full_path

    return files_dict

def get_doc_id_for_row(row: dict[str, str | datetime | float | int]) -> str:
    """
    Get the document id for a given row

    :param row: dict[str, str | datetime | float | int]. Row of data
    :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
    :param toll: str. Toll of the data (2.0TD, 2.0A, 2.0DHA, 2.0DHS...)

    :return: str. Document id
    """
    datetime_spain = row['datetime_spain'].strftime("%Y-%m-%d--%H:00")
    return f"{datetime_spain}--{row['location']}-{row['toll']}"

def get_collection_name(location: str = 'PCB', toll: str = '2.0TD', optimization: str = 'NO-OPTIMIZATION') -> str:
    """
    Get the collection name for a given location and toll

    :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
    :param toll: str. Toll of the data (2.0TD, 2.0A, 2.0DHA, 2.0DHS...)

    :return: str. Collection name
    """
    return f"{location}--{toll}--{optimization}"

def get_collection(client, location: str = 'PCB', toll: str = '2.0TD', aggregation: str = NO_AGGREGATION):
    """
    Returns a collection ref, hidding the subcollections logic
    :param client: Firestore client
    :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
    :param toll: str. Toll of the data (2.0TD, 2.0A, 2.0DHA, 2.0-DHS...)
    :param aggregation: str. Aggregation of the data (NO_AGGREGATION, BY_DAY, BY_MONTH)
    :return: Collection ref
    """
    toll = toll.replace('.', '-')
    global CHECKED_LOCATIONS, CHECKED_TOLLS
    if location not in CHECKED_LOCATIONS[aggregation]:
        # Check if location is an existent document
        document_ref = client.collection(PVPC_PRICES).document(aggregation).collection(LOCATIONS).\
            document(location)
        if not document_ref.get().exists:
            document_ref.set({'name': LOCATION_DESCRIPTIONS[location]})
        CHECKED_LOCATIONS[aggregation].add(location)

    if toll not in CHECKED_TOLLS[aggregation][location]:
        # Check if toll is an existent document
        document_ref = client.collection(PVPC_PRICES).document(aggregation).collection(LOCATIONS).\
            document(location).collection(TOLLS).document(toll)
        if not document_ref.get().exists:
            document_ref.set({'name': toll.replace('-', '.')})
        CHECKED_TOLLS[aggregation][location].add(toll)


    return client.collection(PVPC_PRICES).document(aggregation).collection(LOCATIONS).\
        document(location).collection(TOLLS).document(toll).collection(PRICES)