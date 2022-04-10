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

api_base_url = "https://yh-finance.p.rapidapi.com/market/"

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
s3_trending_name = config['S3']['trending_bucket_name']

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


def upload_charts(data):
    """Convert the dataframes to file object and store them in S3 bucket using boto3

    Args:
        aws (_type_): a tuple of s3-client object and bucket name
        data (_type_): a tuple of indicators dataframe and metadata dataframe

    Returns:
        _type_: two S3 bucket keys for metadata and indicators csv
    """
    df_meta, df = data

    keys = dict(df_meta.iloc[0, :])
    file_name = str(keys['start_date']) + ".csv"

    key = os.path.join(
        keys['symbol'], 'indicators', keys['range'],
        keys['interval'], file_name)

    meta_key = os.path.join(
        keys['symbol'], 'metadata', keys['range'],
        keys['interval'], file_name
    )

    try:
        with io.StringIO() as csv_buffer:
            df_meta.to_csv(csv_buffer, index=False)
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Body=csv_buffer.getvalue(),
                Key=meta_key)
            logging.info(
                f"Successfully uploaded an object to S3 @ s3://{s3_bucket_name}/{meta_key}")

        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Body=csv_buffer.getvalue(),
                Key=key)
            logging.info(
                f"Successfully uploaded an object to S3 @ s3://{s3_bucket_name}/{key}")

    except Exception as e:
        return e

    return (meta_key, key)


def upload_trending(data, starttime):
    key = os.path.join(str(starttime) + '.csv')

    try:
        with io.StringIO() as csv_buffer:
            data.to_csv(csv_buffer, index=False)
            s3_client.put_object(
                Bucket=s3_trending_name, 
                Body=csv_buffer.getvalue(), 
                Key=key)
            logging.info(
                f"Successfully uploaded an object to S3 @ s3://{s3_trending_name}/{key}")
            return key

    except Exception as e:
        return 1


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


def create_trending(response):
    quotes = response['quotes']
    data = {
        'region': [],
        'quoteType': [],
        'marketChangePercent': [],
        'firstTradeDate': [],
        'marketTime': [],
        'marketPrice': [],
        'exchange': [],
        'shortName': [],
        'symbol': []
    }

    for quote in quotes:
        firstTradeDate = datetime.fromtimestamp(
            int(quote.get('firstTradeDateMilliseconds', 0)) / 1000.0).strftime("%Y-%m-%d|%H:%M:%S")
        marketTime = datetime.fromtimestamp(
            int(quote.get('regularMarketTime', 0))).strftime("%Y-%m-%d|%H:%M:%S")
        
        data['region'].append(quote.get('region', "?").replace(",", "-").replace(' ', '-'))
        data['quoteType'].append(quote.get('quoteType', "?").replace(",", "-").replace(' ', '-'))
        data['marketChangePercent'].append(quote.get('regularMarketChangePercent', -1))
        data['firstTradeDate'].append(firstTradeDate)
        data['marketTime'].append(marketTime)
        data['marketPrice'].append(quote.get('regularMarketPrice', -1))
        data['exchange'].append(quote.get('exchange', "?").replace(",", "-").replace(' ', '-'))
        data['shortName'].append(quote.get('shortName', "?").replace(",", "-").replace(' ', '-'))
        data['symbol'].append(quote.get('symbol', "?").replace(",", "-").replace(' ', '-'))

    return pd.DataFrame(data)


def charts():
    """a subpipeline that runs through all above functions
    """
    keys = set([])
    for ticker in tickers:
        api_query_string['symbol'] = ticker

        try:
            response = requests.request(
                "GET", api_base_url + 'get-charts', headers=api_headers, params=api_query_string
            )

            if response.ok:
                logging.info(f'Logging data with query_param={ticker}')
                data = json.loads(response.content)
                data = data['chart']['result'][0]

                try:
                    meta_df = create_metadata(data)
                    df = create_indicators(data)
                    mes = upload_charts((meta_df, df))

                    if type(mes) == "str":
                        logging.error(mes)
                    else:
                        logging.info(
                            f"Successully fetched data with query_param={mes}")
                        keys.add(",".join(mes) + "\n")

                except KeyError:
                    logging.error(f"Found an error with query_param={ticker}")

        except:
            logging.error("Message error: value error")

    with open("/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/pipeline/logs/keys.txt", "w") as file:
        file.writelines(keys)


def trending():
    try:
        response = requests.request(
            "GET", api_base_url + 'get-trending-tickers', headers=api_headers)

        if response.ok:
            logging.info(f'Logging: getting trending tickers')
            data = json.loads(response.content)
            data = data['finance']['result'][0]

            try:
                df = create_trending(data)
                key = upload_trending(df, data['jobTimestamp'])

                if type(key) == "int":
                    logging.error(key)
                else:
                    logging.info(f"Successully fetched trending tickers")
                    with open("/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/pipeline/logs/trending.txt", "w") as file:
                        file.writelines([key])

            except Exception as e:
                logging.error(
                    f"Found an error fetching trending tickers - {e}")

    except:
        logging.error(f"Found an error with fetching trending tickers")


if __name__ == "__main__":
    charts() 
    trending()
