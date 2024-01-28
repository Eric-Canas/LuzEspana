import os
from datetime import datetime
import pandas as pd


def load_csv_as_dicts(csv_path: str, datetime_column_name: str = 'datetime_spain') -> list[dict]:
    """
    Post the content of a csv file to the firebase database

    :param csv_path: str. Path to the csv file to post
    :return: list[dict]. List of dictionaries with the data from the csv file
    """
    # Leer el archivo CSV
    df = pd.read_csv(filepath_or_buffer=csv_path, sep=',')

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

def get_doc_id_for_row(row: dict[str, str | datetime | float | int], location: str = 'PCB') -> str:
    """
    Get the document id for a given row

    :param row: dict[str, str | datetime | float | int]. Row of data
    :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
    :param toll: str. Toll of the data (2.0TD, 2.0A, 2.0DHA, 2.0DHS...)

    :return: str. Document id
    """
    datetime_spain = row['datetime_spain'].strftime("%Y-%m-%d--%H:00")
    return f"{datetime_spain}--{location}-{row['toll']}"
