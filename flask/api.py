import psycopg2
import configparser
from flask import Flask
from flask_cors import CORS

import query
api = Flask(__name__)
CORS(api)

config = configparser.ConfigParser()
config.read("/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/config.cfg")

conn = psycopg2.connect(host=config['Postgres']['host'],
                        dbname=config['Postgres']['dbName'],
                        user=config['Postgres']['user'],
                        password=config['Postgres']['password'],
                        port=config['Postgres']['port'])
cur = conn.cursor()


@api.route("/")
def root():
    return "Hello World"


@api.route('/profile')
def my_profile():
    return {
        "name": "Dung Ngo",
        "about": "Hello! I'm a full stack developer that loves python and javascript"
    }


@api.route("/table")
def table():
    def to_json(lists):
        from collections import defaultdict
        res = defaultdict(dict)
        for lst in lists:
            res[lst[8]] = {
                'name': lst[7],
                'exchange': lst[6],
                'price': lst[5],
                'time': lst[4].strftime("%Y-%m-%d %H:%M:%S"),
                'firstTrade': lst[3].strftime("%Y-%m-%d %H:%M:%S"),
                'percent': lst[2],
                'quoteType': lst[1],
                'region': lst[0]
            }
        return res

    cur.execute(query.select_latest_tickers)
    data = cur.fetchall()
    return to_json(data)


if __name__ == "__main__":
    api.run(debug=True)
