import logging

import backend.config as config
import flask
import flask_cors
from flask import request
from pipeline.fetch import charts, profile
import pipeline.postgres as postgres

import endpoints.query as query

logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S',
                    filename=config.api_log_path,
                    level=logging.INFO)
api = flask.Flask(__name__)
flask_cors.CORS(api)


def to_timeseries(data):
    res = {'low': [], 'open': [], 'high': [], 'close': [], 'datetime': []}
    for row in data:
        res['low'].append(row[0])
        res['open'].append(row[1])
        res['high'].append(row[2])
        res['close'].append(row[3])
        res['datetime'].append(row[4])

    return res


def to_trending(lists):
    if not (type(lists) == list):
        return {}

    res = {}
    for lst in lists:
        cur = config.conn.cursor()

        cur.execute(query.select_latest_charts.format(ticker=lst[8]))
        data = cur.fetchall()
        ts = to_timeseries(data)

        res[lst[8]] = {
            'name': lst[7],
            'exchange': lst[6],
            'price': round(lst[5]),
            'time': lst[4].strftime("%Y-%m-%d %H:%M:%S"),
            'percent': round(lst[2], 2),
            'quoteType': lst[1],
            'timeseries': ts
        }
    return res


def to_profile(profile):
    if len(profile) < 21:
        return {}

    return {
        'openPrice': profile[0],
        'exchangeName': profile[1],
        'marketTime': profile[2],
        'name': profile[3],
        'currency': profile[4],
        'marketCap': profile[5],
        'quoteType': profile[6],
        'exchangeTimezoneName': profile[7],
        'beta': profile[8],
        'yield': profile[9],
        'dividendRate': profile[10],
        'strikePrice': profile[11],
        'ask': profile[12],
        'sector': profile[13],
        'fullTimeEmployees': profile[14],
        'longBusinessSummary': profile[15],
        'city': profile[16],
        'country': profile[17],
        'website': profile[18],
        'industry': profile[19],
        'symbol': profile[20]
    }


def to_news(data):
    res = []
    for tuple in data:
        if not tuple:
            continue

        res.append({
            "id": tuple[0],
            "contentType": tuple[1],
            "title": tuple[2],
            "pubDate": tuple[3],
            "thumbnailUrl": tuple[4],
            "thumbnailWidth": tuple[5],
            "thumbnailHeight": tuple[6],
            "thumbailTag": tuple[7],
            "Url": tuple[8],
            "provider": tuple[9]
        })
    return res


@api.route("/")
def root():
    return {
        "name":
        "Dung Ngo",
        "about":
        "Hello! I'm a full stack developer that loves python and javascript"
    }


@api.route("/trending")
def table():
    cur = config.conn.cursor()
    cur.execute(query.select_latest_tickers)
    data = cur.fetchall()
    return to_trending(data)


@api.route("/movers")
def movers():
    cur = config.conn.cursor()

    data = {}
    for ticker in ["AAPL", "MSFT", "AMZN", "ZM", "COIN"]:
        cur.execute(query.select_top_5_ticker_charts.format(ticker=ticker))
        ts = to_timeseries(cur.fetchall())

        cur.execute(query.select_top_5_ticker_profiles.format(ticker=ticker))
        profile = cur.fetchone()

        if ts and profile:
            data[ticker] = {'timeseries': ts, 'profile': to_profile(profile)}

    return data


@api.route("/news")
def news():
    cur = config.conn.cursor()
    cur.execute(query.select_news)
    data = cur.fetchall()
    return {"response": to_news(data)}


@api.post('/search')
def search():
    ticker = request.args.get('ticker')

    charts([ticker])
    profile([ticker])

    postgres.metadata()
    postgres.indicators()
    postgres.profile()

    res = {}
    cur = config.conn.cursor()
    cur.execute(query.select_one_ticker.format(ticker=ticker))
    d = cur.fetchone()
    print(d)
    if d == None:
        return res

    cur.execute(query.select_latest_charts.format(ticker=ticker))
    ts = cur.fetchall()

    res[d[20]] = {
        'name': d[3],
        'exchange': d[1],
        'price': round(d[0]),
        'time': d[2].strftime("%Y-%m-%d %H:%M:%S"),
        'percent': 1,
        'quoteType': d[6],
        'timeseries': to_timeseries(ts)
    }
    return res


if __name__ == "__main__":
    api.run(debug=True)
