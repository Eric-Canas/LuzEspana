from datetime import datetime

from data_management.firebase.firebase_manager import FirebaseManager
from data_management.firebase.firebase_querier import FirebaseQuerier
from data_management.prices_downloader import PricesDownloader

if __name__ == '__main__':

    """
    prices_downloader = PricesDownloader()
    date = datetime.now()
    date = datetime(year=2022, month=1, day=1)
    past_date = datetime(year=2020, month=1, day=1) #date - timedelta(days=10)
    paths = prices_downloader.download_prices_for_date_range(start_date=past_date, end_date=date)
    """

    date = datetime.now()
    past_date = datetime(year=2020, month=1, day=1) #date - timedelta(days=10)
    firebase_manager = FirebaseManager()
    firebase_manager.post_for_date_range(start_date=past_date, end_date=date)


    date = datetime.now()
    past_date = datetime(year=2023, month=6, day=1) #date - timedelta(days=10)
    firebase_querier = FirebaseQuerier()
    data = firebase_querier.avg_price_between_dates_by_hour(start_date=past_date, end_date=date)
    print(data)
