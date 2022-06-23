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

base_path = '/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend'
log_path = f"{base_path}/pipeline/logs/postgres.log"
config_path = f"{base_path}/config.cfg"
indicators_txt_path = f"{base_path}/pipeline/buffer/indicators.txt"
meta_txt_path = f"{base_path}/pipeline/buffer/meta.txt"
trending_txt_path = f"{base_path}/pipeline/buffer/trending.txt"
profile_txt_path = f"{base_path}/pipeline/buffer/profile.txt"
news_txt_path = f"{base_path}/pipeline/buffer/news.txt"

logging.basicConfig(filename=log_path,level=logging.INFO)

config = configparser.ConfigParser()
config.read(config_path)

api_base_url = "https://yh-finance.p.rapidapi.com/market/"
api_base_url_v2 = "https://yh-finance.p.rapidapi.com/stock/v2/"
api_base_url_news = "https://yh-finance.p.rapidapi.com/news/v2/"

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
s3_profile_name = config['S3']['profile_bucket_name']
s3_news_name = config['S3']['news_bucket_name']


def upload_charts(data):
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
        return (-1, -1)

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


def upload_profile(data, symbol):
    key = os.path.join(symbol + '.csv')

    try:
        with io.StringIO() as csv_buffer:
            data.to_csv(csv_buffer, index=False)
            s3_client.put_object(
                Bucket=s3_profile_name,
                Body=csv_buffer.getvalue(),
                Key=key)
            logging.info(
                f"Successfully uploaded an object to S3 @ s3://{s3_profile_name}/{key}")
            return key

    except Exception as e:
        logging.error(e)
        return 1


def upload_news(data):
    time = datetime.now().strftime("%Y-%m-%d|%H:%M:%S")
    key = os.path.join(time + '.csv')

    try:
        with io.StringIO() as csv_buffer:
            data.to_csv(csv_buffer, index=False)
            s3_client.put_object(
                Bucket=s3_news_name,
                Body=csv_buffer.getvalue(),
                Key=key)
            logging.info(
                f"Successfully uploaded an object to S3 @ s3://{s3_news_name}/{key}")
            return key

    except Exception as e:
        logging.error(e)
        return 1


def create_indicators(response):
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

    return df_indicators.fillna(method='bfill')


def create_metadata(response):
    metadata = response['meta']
    timestamps = response['timestamp']

    def get_trading_period(trade_period):
        start_period = 0
        end_period = 0

        for period in trade_period.values():
            start = datetime.fromtimestamp(int(
                period[0][0]['start'])).strftime("%H:%M:%S")
            end = datetime.fromtimestamp(int(
                period[0][0]['end'])).strftime("%H:%M:%S")

            start_period = start if start_period == 0 else min(
                start_period, start)
            end_period = end if end_period == 0 else max(end_period, end)

        date = f"{start_period}|{end_period}"
        return date

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

    return pd.DataFrame(impt_metadata, index=[0])


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

        data['region'].append(
            quote.get('region', "?").replace(",", "-").replace(' ', '-'))
        data['quoteType'].append(
            quote.get('quoteType', "?").replace(",", "-").replace(' ', '-'))
        data['marketChangePercent'].append(
            quote.get('regularMarketChangePercent', -1))
        data['firstTradeDate'].append(firstTradeDate)
        data['marketTime'].append(marketTime)
        data['marketPrice'].append(quote.get('regularMarketPrice', -1))
        data['exchange'].append(
            quote.get('exchange', "?").replace(",", "-").replace(' ', '-'))
        data['shortName'].append(
            quote.get('shortName', "?").replace(",", "-").replace(' ', '-'))
        data['symbol'].append(
            quote.get('symbol', "?").replace(",", "-").replace(' ', '-'))

    return pd.DataFrame(data)


def create_profile(data):
    price = data['price']
    quoteType = data['quoteType']
    summaryDetail = data['summaryDetail']
    assetProfile = data['assetProfile']

    df = {'symbol': data['symbol']}
    df['openPrice'] = price['regularMarketOpen']['raw']
    df['exchangeName'] = price['exchangeName'].replace(
        " ", "-").replace(',', "-")
    df['marketTime'] = datetime.fromtimestamp(
        price['regularMarketTime']).strftime("%Y-%m-%d|%H:%M:%S")
    df['name'] = price['shortName'].replace(" ", "-").replace(',', "-")
    df['currency'] = price['currency']
    df['marketCap'] = price['marketCap'].get('fmt', -1)
    df['quoteType'] = price['quoteType'].replace(" ", "-").replace(',', "-")

    df['exchangeTimezoneName'] = quoteType['exchangeTimezoneName']

    df['beta'] = summaryDetail['beta'].get("fmt", -1)
    df['yield'] = summaryDetail['yield'].get("fmt", -1)
    df['dividendRate'] = summaryDetail['dividendRate'].get("fmt", -1)
    df['strikePrice'] = summaryDetail['strikePrice'].get('fmt', -1)
    df['ask'] = summaryDetail['ask'].get('fmt', -1)

    df['sector'] = assetProfile['sector'].replace(" ", "-").replace(',', "-")
    df['fullTimeEmployees'] = assetProfile['fullTimeEmployees']
    df['longBusinessSummary'] = assetProfile['longBusinessSummary'].replace(
        " ", "-").replace(',', "-")
    df['city'] = assetProfile['city'].replace(" ", "-").replace(',', "-")
    df['country'] = assetProfile['country'].replace(" ", "-").replace(',', "-")
    df['website'] = assetProfile['website']
    df['industry'] = assetProfile['industry'].replace(
        " ", "-").replace(',', "-")

    return pd.DataFrame(df, index=[0])


def create_news(data):
    def resolutions(thumbnail):
        if thumbnail == None:
            return {'url': '', 'width': 0, 'height': 0, 'tag': ''}
        
        group = []
        for reso in thumbnail['resolutions']:
            url = reso['url']
            w = reso['width']
            h = reso['height']
            tag = reso['tag']
            group.append(
                ((w, h), {'url': url, 'width': w, 'height': h, 'tag': tag}))

        sort = sorted(group, key=lambda x: x[0])
        return sort[0][1]

    news = data['data']['main']['stream']

    df = {
        'id': [],
        'contentType': [],
        'title': [],
        'pubDate': [],
        'thumbnailUrl': [],
        'thumbnailWidth': [],
        'thumbnailHeight': [],
        'thumbailTag': [],
        'Url': [],
        'provider': []
    }

    for info in news:
        content = info['content']
        thumbnail = resolutions(content['thumbnail'])

        df['id'].append(info['id'])
        df['contentType'].append(content['contentType'])
        df['title'].append(content['title'])
        df['pubDate'].append(content['pubDate'])
        df['thumbnailUrl'].append(thumbnail['url'])
        df['thumbnailWidth'].append(thumbnail['width'])
        df['thumbnailHeight'].append(thumbnail['height'])
        df['thumbailTag'].append(thumbnail['tag'])
        df['Url'].append(content['clickThroughUrl']['url'] if content['clickThroughUrl'] != None else '')
        df['provider'].append(content['provider']['displayName'])

    return pd.DataFrame(df)


def charts(tickers):
    """a subpipeline that runs through all above functions
    """
    api_query_string = {
        "region": "US",
        "range": config['env']['rangeData'],
        "interval": config['env']['interval']
    }
    meta_keys = set([])
    indicators_keys = set([])

    for ticker in tickers:
        api_query_string['symbol'] = ticker

        try:
            response = requests.request(
                "GET", 
                api_base_url_v2 + 'get-chart', 
                headers=api_headers, 
                params=api_query_string)

            if response.ok:
                logging.info(f'Logging data with query_param={ticker}')
                data = json.loads(response.content)
                data = data['chart']['result'][0]

                try:
                    meta_df = create_metadata(data)
                    df = create_indicators(data)
                    meta_key, indicators_key = upload_charts((meta_df, df))

                    if type(meta_key) == "int":
                        logging.error("Error of parsing charts")
                    else:
                        logging.info(f"Successully fetched data with query_param={meta_key}-{indicators_key}")
                        meta_keys.add(meta_key + "\n")
                        indicators_keys.add(indicators_key + "\n")

                except Exception as e:
                    logging.error(f"Found a parsing error with query_param={ticker}: {e}")

        except Exception as e:
            logging.error(f"Found a fetching error with query_param={ticker}: {e}")

    with open(meta_txt_path, "w") as file:
        file.writelines(meta_keys)
    with open(indicators_txt_path, "w") as file:
        file.writelines(indicators_keys)


def trending():
    try:
        response = requests.request(
            "GET", 
            api_base_url + 'get-trending-tickers', 
            headers=api_headers)

        if response.ok:
            movers = ["AAPL", "MSFT", "AMZN", "FB", "COIN"]
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
                    with open(trending_txt_path, "w") as file:
                        file.writelines([key])

                charts(df['symbol'].values.tolist() + movers)
                profile(df['symbol'].values.tolist() + movers)

            except Exception as e:
                logging.error(f"Found an error fetching trending tickers - {e}")

    except:
        logging.error(f"Found an error with fetching trending tickers")


def profile(tickers):
    keys = set([])
    api_query_string = {"region": "US"}

    for ticker in tickers:
        api_query_string['symbol'] = ticker
        try:
            response = requests.request(
                "GET", 
                api_base_url_v2 + 'get-profile',
                headers=api_headers,
                params=api_query_string)

            if response.ok:
                logging.info(f"Logging: getting tickers' profile")
                data = json.loads(response.content)

                try:
                    df = create_profile(data)
                    key = upload_profile(df, ticker)

                    if type(key) == "int":
                        logging.error(key)
                    else:
                        logging.info(f"Successully fetched tickers' profile")

                    keys.add(key + "\n")

                except Exception as e:
                    logging.error(f"Found an error fetching tickers' profile - {e}")

        except:
            logging.error(f"Found an error with fetching tickers' profile")

    with open(profile_txt_path, "w") as file:
        file.writelines(keys)


def news():
    keys = set([])
    api_query_string = {
        "region": "US",
        'snippetCount': '50',
    }
    api_headers['Content-Type'] = 'text/plain'

    try:
        response = requests.request(
            "POST", 
            api_base_url_news + 'list', 
            headers=api_headers, 
            params=api_query_string)

        if response.ok:
            logging.info(f"Logging: getting latest news")
            data = json.loads(response.content)
            try:
                df = create_news(data)
                key = upload_news(df)

                if type(key) == "int":
                    logging.error(key)
                else:
                    logging.info(f"Successully fetched latest news")

                keys.add(key + "\n")

            except Exception as e:
                logging.error(
                    f"Found an error fetching latest news - {e}")

    except Exception as e:
        logging.error(f"Found an error fetching latest news - {e}")

    with open(news_txt_path, "w") as file:
        file.writelines(keys)


if __name__ == "__main__":
    trending()
    news()
