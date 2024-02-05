import os
import numpy as np
from collections import defaultdict
from datetime import date, datetime
from google.cloud.firestore_v1 import FieldFilter
import firebase_admin
from firebase_admin import credentials as firebase_crendentials, firestore

from data_management.constants import BY_DAY
from utils.utils import get_collection_name, get_doc_id_for_row, get_collection

CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "resources", "credentials",
                                "electric-bill-backtesting-firebase-adminsdk-d44q1-c89a4a1bb7.json")

PERIOD_COLORS = {
    1: 'red',
    2: 'orange',
    3: 'green'
}

class FirebaseQuerier():
    def __init__(self):
        credentials = firebase_crendentials.Certificate(cert=CREDENTIALS_PATH)
        firebase_admin.initialize_app(credential=credentials)
        self.client = firestore.client()


    def avg_price_between_dates_by_period(self, start_date: date, end_date: date,
                                          location: str = 'PCB', toll: str = '2.0TD') -> dict[int, float]:
        """
        Get the average price between 2 dates by period


        :param start_date: date. Start date of the range
        :param end_date: date. End date of the range
        :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
        :param toll: str. Toll of the data (2.0TD, 2.0A, 2.0DHA, 2.0-DHS...)

        :return: dict[int, float]. Dict with the average price by period
        """
        collection_ref = get_collection(client=self.client, location=location, toll=toll, aggregation=BY_DAY)

        # Build the timestamps to cover the whole day
        start_timestamp = datetime.combine(start_date, datetime.min.time())
        end_timestamp = datetime.combine(end_date, datetime.max.time())

        # Query Firestore
        query = collection_ref.where(filter=FieldFilter(field_path='datetime_spain', op_string='>=', value=start_timestamp)).\
                               where(filter=FieldFilter(field_path='datetime_spain', op_string='<=', value=end_timestamp)).\
                               select(['period', 'PVPC_price_kwh', 'day']).stream()
        docs = [doc.to_dict() for doc in query]
        period_prices = defaultdict(list)
        for day in docs:
            periods, prices = day['period'], day['PVPC_price_kwh']
            assert len(periods) == len(prices), f"Periods and prices should have the same length. Got {len(periods)} periods and {len(prices)} prices"
            for period, price in zip(periods, prices):
                period_prices[period].append(price)

        # Calculate averages
        avg_prices = {}
        for period, prices in period_prices.items():
            avg_prices[period] = sum(prices) / len(prices)

        return avg_prices

    def avg_price_between_dates_by_hour(self, start_date: date, end_date: date,
                                          location: str = 'PCB', toll: str = '2.0TD',
                                        plot: bool = True) -> dict[int, float]:
        """
        Get the average price between 2 dates by period


        :param start_date: date. Start date of the range
        :param end_date: date. End date of the range
        :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
        :param toll: str. Toll of the data (2.0TD, 2.0A, 2.0DHA, 2.0-DHS...)

        :return: dict[int, float]. Dict with the average price by period
        """
        collection_ref = get_collection(client=self.client, location=location, toll=toll, aggregation=BY_DAY)

        # Build the timestamps to cover the whole day
        start_timestamp = datetime.combine(start_date, datetime.min.time())
        end_timestamp = datetime.combine(end_date, datetime.max.time())

        # Query Firestore
        query = collection_ref.where(filter=FieldFilter(field_path='datetime_spain', op_string='>=', value=start_timestamp)).\
                               where(filter=FieldFilter(field_path='datetime_spain', op_string='<=', value=end_timestamp)).\
                               select(['period', 'PVPC_price_kwh', 'day']).stream()
        docs = [doc.to_dict() for doc in query]
        # Build a matrix of 24xHour
        hour_prices = []
        for doc in docs:
            hour_prices.append(doc['PVPC_price_kwh'])
        hour_prices = np.array(hour_prices)
        avg_prices = np.mean(hour_prices, axis=0)
        prices_by_hour = {i: avg_prices[i] for i in range(24)}
        if plot:
            import matplotlib.pyplot as plt
            periods_by_hour = docs[0]['period']
            colors = [PERIOD_COLORS[period] for period in periods_by_hour]
            # Use the period by hour as color for the bars
            plt.bar(prices_by_hour.keys(), prices_by_hour.values(), color=colors)
            # Draw the value of the average price
            plt.axhline(y=np.mean(list(prices_by_hour.values())), color='r', linestyle='-')
            # Write the value of each bar
            for i, price in prices_by_hour.items():
                plt.text(i, round(price, 3), f'{price:.2f}', ha='center', va='bottom')
            plt.xlabel('Hour')
            plt.ylabel('Average price')
            plt.title('Average price by hour')
            # Show all x-ticks
            plt.xticks(range(24))
            plt.show()

        return prices_by_hour



    def get_data_for_day(self, day: date, location: str = 'PCB', toll: str = '2.0TD') -> \
            list[dict[str, str | datetime | float | int]]:
        """
        Get the data for a given day from the database, determining a location and a toll.
        :param day: date. Day of the data to get (example: datetime(year=2021, month=6, day=1))
        :param location: str. Location of the data [PCB (Peninsula, Canarias, Baleares) or CYM (Ceuta, Melilla)]
        :param toll: str. Toll of the data (2.0TD, 2.0A, 2.0.DHA, 2.0.DHS...)

        :return: list[dict[str, str | datetime | float | int]]. List of dictionaries with the data for the given day
        """
        collection_ref = get_collection(client=self.client, location=location, toll=toll, aggregation=BY_DAY)
        doc_id = get_doc_id_for_row(row={'datetime_spain': day, 'location': location, 'toll': toll})

        # Get the document
        doc = collection_ref.document(doc_id).get()
        if not doc.exists:
            return None
        return doc.to_dict()
