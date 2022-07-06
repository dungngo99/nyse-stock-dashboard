from pipeline.fetch_api import fetch_charts, fetch_news, fetch_profile, fetch_trending
from pipeline.load_csv import load_csv
from pipeline.update_pg import update_postgres


def main():
    tickers = [
        "ZM",
        "COIN",
        "NFLX",
        "AAPL",
        "GOOG",
        "MSFT",
        "MS",
        "GS",
        "TSLA",
        "DASH",
        "BAC",
        "ROKU",
        "TWRT",
    ]
    # fetch_charts(tickers)
    # fetch_profile(tickers)
    # fetch_trending()
    # fetch_news()
    load_csv()
    update_postgres()


if __name__ == "__main__":
    main()
