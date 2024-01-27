from datetime import datetime
from data_management.prices_downloader import PricesDownloader

if __name__ == '__main__':

    prices_downloader = PricesDownloader()
    date = datetime(year=2024, month=1, day=1)
    file = prices_downloader.download_day(date=date)

    print(f"File downloaded to {file}")