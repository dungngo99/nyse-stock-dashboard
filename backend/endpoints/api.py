import psycopg2
import configparser
import flask
import flask_cors

import query
api = flask.Flask(__name__)
flask_cors.CORS(api)

config_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/config.cfg"
config = configparser.ConfigParser()
config.read(config_path)

conn = psycopg2.connect(host=config['Postgres']['host'],
                        dbname=config['Postgres']['dbName'],
                        user=config['Postgres']['user'],
                        password=config['Postgres']['password'],
                        port=config['Postgres']['port'])


def to_timeseries(data):
    res = {
        'low': [],
        'open': [],
        'high': [],
        'close': [],
        'datetime': []
    }
    for row in data:
        res['low'].append(row[0])
        res['open'].append(row[1])
        res['high'].append(row[2])
        res['close'].append(row[3])
        res['datetime'].append(row[4])

    return res


def to_json(lists):
    res = {}
    for lst in lists:
        cur = conn.cursor()

        cur.execute(query.select_latest_charts.format(ticker=lst[8]))
        data = cur.fetchall()
        ts = to_timeseries(data)

        if len(ts['datetime']) == 0:
            continue

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
        "name": "Dung Ngo",
        "about": "Hello! I'm a full stack developer that loves python and javascript"
    }


@api.route("/trending")
def table():
    cur = conn.cursor()
    cur.execute(query.select_latest_tickers)
    data = cur.fetchall()
    return to_json(data)


@api.route("/movers")
def movers():
    cur = conn.cursor()

    data = {}
    for ticker in ["AAPL", "MSFT", "AMZN", "FB", "COIN"]:
        cur.execute(query.select_top_5_ticker_charts.format(ticker=ticker))
        ts = to_timeseries(cur.fetchall())
        cur.execute(query.select_top_5_ticker_profiles.format(ticker=ticker))
        profile = cur.fetchone()
        data[ticker] = {'timeseries': ts, 'profile': to_profile(profile)}

    return data


@api.route("/news")
def news():
    cur = conn.cursor()
    cur.execute(query.select_news)
    data = cur.fetchall()
    return {"response": to_news(data)}


if __name__ == "__main__":
    api.run(debug=True)
