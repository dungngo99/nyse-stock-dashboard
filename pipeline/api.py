import requests
import json
import configparser
import boto3
import logging
import pandas as pd
import os
import io
import logging
from datetime import datetime

logging.basicConfig(
    filename='/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/pipeline/logs/api.log',
    level=logging.INFO
)

config = configparser.ConfigParser()
config.read("/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/config.cfg")

api_base_url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/market/get-charts"

api_query_string = {
    "region": "US",
    "lang": "en",
    "range": config['env']['rangeData'],
    "interval": config['env']['interval']
}

api_headers = {
    'x-rapidapi-host': config["RapidAPI"]['x-rapidapi-host'],
    'x-rapidapi-key': config['RapidAPI']['x-rapidapi-key'],
    'Content-Type': "application/json"
}

s3_client = boto3.client(
    "s3",
    region_name='us-west-2',
    aws_access_key_id=config['AWS']['aws_access_key_id'],
    aws_secret_access_key=config['AWS']['aws_secret_access_key']
)

s3_bucket_name = config['S3']['bucket_name']

tickers = set([
    'AAPL',
    'MSFT',
    'ABNB',
    'ACN',
    'ADBE',
    'TSLA',
    'FB',
    'COIN',
    'DKNG',
    'JPM',
    'AMZN',
    'GOOGL',
    'BAC',
    'PPE',
    'MA',
    'F',
    'NVDA',
    'VOO'
])


def upload_object(aws, data, tag):
    """Convert the dataframes to file object and store them in S3 bucket using boto3

    Args:
        aws (_type_): a tuple of s3-client object and bucket name
        data (_type_): a tuple of indicators dataframe and metadata dataframe
        tag (_type_): a string either "indicators" or "metadata"

    Returns:
        _type_: two S3 bucket keys for metadata and indicators csv
    """
    s3, bucket = aws
    df_meta, df = data

    keys = dict(df_meta.iloc[0, :])
    file_name = str(keys['start_date']) + f".csv"

    key = os.path.join(
        keys['symbol'], tag, keys['range'],
        keys['interval'], file_name)

    meta_key = os.path.join(
        keys['symbol'], 'metadata', keys['range'],
        keys['interval'], file_name
    )

    try:
        with io.StringIO() as csv_buffer:
            df_meta.to_csv(csv_buffer, index=False)
            s3.put_object(
                Bucket=bucket, Body=csv_buffer.getvalue(), Key=meta_key)
            logging.info(
                f"Successfully uploaded an object to S3 @ s3://{bucket}/{meta_key}")

        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            s3.put_object(
                Bucket=bucket, Body=csv_buffer.getvalue(), Key=key)
            logging.info(
                f"Successfully uploaded an object to S3 @ s3://{bucket}/{key}")

    except Exception as e:
        return e

    return (meta_key, key)


def create_indicators(response):
    """Create an indicators dataframe from the json response

    Args:
        response (_type_): a dictionary that stores fetched data

    Returns:
        _type_: a dataframe
    """
    metadata = response['meta']
    timestamps = response['timestamp']
    indicators = response['indicators']['quote'][0]

    table = {
        "Timestamps": timestamps,
        "Volume": indicators['volume'],
        "Low": indicators["low"],
        "Open": indicators["open"],
        "High": indicators["high"],
        "Close": indicators["close"]
    }

    tss = pd.Series(timestamps).apply(
        lambda x: datetime.fromtimestamp(int(x)).strftime("%Y-%m-%d|%H:%M:%S"))

    df_indicators = pd.DataFrame(table)
    df_indicators['Datetime'] = tss
    df_indicators['symbol'] = metadata['symbol']

    return df_indicators


def create_metadata(response):
    """Create a metadata dataframe from the json response

    Args:
        response (_type_): a dictionary that stores fetched data

    Returns:
        _type_: a dataframe 
    """
    metadata = response['meta']
    timestamps = response['timestamp']

    def get_trading_period(trade_period):
        trading_periods = set([])
        for period in trade_period:
            start = datetime.fromtimestamp(int(
                period[0]['start'])).strftime("%H:%M:%S")
            end = datetime.fromtimestamp(int(
                period[0]['end'])).strftime("%H:%M:%S")
            date = f"{start}|{end}"
            trading_periods.add(date)
        return list(trading_periods)

    tss = pd.Series(timestamps).apply(
        lambda x: datetime.fromtimestamp(int(x)).strftime("%Y-%m-%d|%H:%M:%S"))

    impt_metadata = {
        "currency": metadata['currency'],
        "symbol": metadata['symbol'],
        "instrumentType": metadata['instrumentType'],
        "firstTradeDate": datetime.fromtimestamp(
            metadata['firstTradeDate']).strftime("%Y-%m-%d|%H:%M:%S"),
        "exchangeTimezoneName": metadata["exchangeTimezoneName"],
        'timezone': metadata['timezone'],
        'trade_period': get_trading_period(metadata['tradingPeriods']),
        'range': metadata['range'],
        'interval': metadata['dataGranularity'],
        'start_date': tss.min()
    }

    return pd.DataFrame(impt_metadata)


def api():
    """a subpipeline that runs through all above functions
    """
    keys = set([])
    for ticker in tickers:
        api_query_string['symbol'] = ticker

        try:
            response = requests.request(
                "GET", api_base_url, headers=api_headers, params=api_query_string
            )

            if response.ok:
                logging.info(f'Logging data with query_param={ticker}')
                data = json.loads(response.content)
                data = data['chart']['result'][0]

                try:
                    meta_df = create_metadata(data)
                    df = create_indicators(data)

                    mes = upload_object(
                        (s3_client, s3_bucket_name),
                        (meta_df, df), "indicators"
                    )
                except KeyError:
                    logging.error(f"Found an error with query_param={ticker}")

                if type(mes) == "str":
                    logging.error(mes)
                else:
                    logging.info(
                        f"Successully fetched data with query_param={mes}")
                    keys.add(",".join(mes) + "\n")

        except ValueError:
            logging.error("Message error: value error")

    with open("/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/pipeline/logs/keys.txt", "w") as file:
        file.writelines(keys)


if __name__ == "__main__":
    api()
