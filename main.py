import os
from datetime import datetime, timedelta

from data_management.firebase_manager import FirebaseManager
from data_management.prices_downloader import PricesDownloader

if __name__ == '__main__':
    """
    prices_downloader = PricesDownloader()
    date = datetime.now()
    past_date = datetime(year=2021, month=1, day=1) #date - timedelta(days=10)
    paths = prices_downloader.download_prices_for_date_range(start_date=past_date, end_date=date)
    print(paths)
    """


    firebase_manager = FirebaseManager()
    firebase_manager.post_day_of_data(day=datetime(year=2021, month=1, day=1))