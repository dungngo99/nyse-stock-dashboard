import json
import logging
import os
from datetime import datetime

import backend.config as config
import requests

logging.basicConfig(filename=config.fetch_log_path, level=logging.INFO, filemode="w")


# Chart API
def fetch_charts(tickers):
    charts_keys = set([])
    api_query_string = {
        "region": "US",
        "range": config.configs["env"]["rangeData"],
        "interval": config.configs["env"]["interval"],
    }

    for ticker in tickers:
        api_query_string["symbol"] = ticker

        logging.info(f" Fetching charts for query parameter={ticker}")

        try:
            response = requests.request(
                "GET",
                config.api_stock_v2_url + "get-chart",
                headers=config.api_headers,
                params=api_query_string,
            )

            if response.ok:
                data = json.loads(response.content)
                chart_key = upload_charts(data, ticker)
                charts_keys.add(chart_key + "\n")
                logging.info(f" Finishing fetching charts for query parameter={ticker}")
        except Exception as e:
            logging.error(f"Error fetching ticker {ticker}'s charts: {e}")

    with open(config.charts_txt_path, "w") as file:
        file.writelines(charts_keys)


def upload_charts(data, ticker):
    range = config.configs["env"]["rangeData"]
    interval = config.configs["env"]["interval"]
    date = datetime.now().strftime("%Y-%m-%d|%H:%M:%S")

    key = os.path.join(ticker, "charts", range, interval, date + ".json")
    bucket = config.s3_bucket_name
    config.s3_client.put_object(Bucket=bucket, Body=json.dumps(data), Key=key)

    logging.info(f" Uploaded a chart.json to S3 @ s3://{bucket}/{key}")
    return key


# Trending API
def fetch_trending():
    try:
        response = requests.request(
            "GET",
            config.api_market_url + "get-trending-tickers",
            headers=config.api_headers,
        )

        if response.ok:
            logging.info(f" Fetching trending tickers")
            data = json.loads(response.content)

            key = upload_trending(data)
            with open(config.trending_txt_path, "w") as file:
                file.writelines(key)

            logging.info(f" Finished fetching trending tickers")
    except Exception as e:
        logging.error(f"Error fetching trending tickers: {e}")


def upload_trending(data):
    key = datetime.now().strftime("%Y-%m-%d|%H:%M:%S") + ".json"
    config.s3_client.put_object(
        Bucket=config.s3_trending_name, Body=json.dumps(data), Key=key
    )
    logging.info(
        f" Uploaded a trending.json to S3 @ s3://{config.s3_trending_name}/{key}"
    )
    return key


# Profile API
def fetch_profile(tickers):
    keys = set([])
    api_query_string = {"region": "US"}

    for ticker in tickers:
        api_query_string["symbol"] = ticker

        logging.info(f" Fetching ticker {ticker}'s profile")

        try:
            response = requests.request(
                "GET",
                config.api_stock_v2_url + "get-profile",
                headers=config.api_headers,
                params=api_query_string,
            )

            if response.ok:
                data = json.loads(response.content)
                key = upload_profile(data, ticker)

                logging.info(f" Finished fetching ticker {ticker}'s profile")
                keys.add(key + "\n")
        except Exception as e:
            logging.error(f"Error fetching ticker {ticker}'s profile: {e}")

    with open(config.profile_txt_path, "w") as file:
        file.writelines(keys)


def upload_profile(data, ticker):
    key = ticker + ".json"
    config.s3_client.put_object(
        Bucket=config.s3_profile_name, Body=json.dumps(data), Key=key
    )
    logging.info(
        f" Uploaded a trending.json to S3 @ s3://{config.s3_profile_name}/{key}"
    )
    return key


# News API
def fetch_news():
    import copy

    api_query_string = {
        "region": "US",
        "snippetCount": "28",
    }
    headers = copy.deepcopy(config.api_headers)
    headers["content-type"] = "text/plain"

    try:
        logging.info(f" Fetching latest news")
        response = requests.request(
            "POST",
            config.api_news_url + "list",
            headers=headers,
            params=api_query_string,
        )

        if response.ok:
            data = json.loads(response.content)
            key = upload_news(data)
            logging.info(f" Finished fetching latest news")

            with open(config.news_txt_path, "w") as file:
                file.writelines(key)
    except Exception as e:
        logging.error(f"Error fetching latest news: {e}")


def upload_news(data):
    key = datetime.now().strftime("%Y-%m-%d|%H:%M:%S") + ".json"
    config.s3_client.put_object(
        Bucket=config.s3_news_name, Body=json.dumps(data), Key=key
    )
    logging.info(f" Uploaded a news.json to S3 @ s3://{config.s3_news_name}/{key}")
    return key


if __name__ == "__main__":
    fetch_charts(["AAPL"])
    fetch_news()
    fetch_profile(["AAPL"])
    fetch_trending()
