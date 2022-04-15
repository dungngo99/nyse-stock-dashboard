import psycopg2
import configparser
import query

config = configparser.ConfigParser()
config.read("/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/config.cfg")


def fetch_postgres():
    """A data-quality that simply fetches queried data
    """
    conn = psycopg2.connect(host=config['Postgres']['host'],
                            dbname=config['Postgres']['dbName'],
                            user=config['Postgres']['user'],
                            password=config['Postgres']['password'],
                            port=config['Postgres']['port'])
    cur = conn.cursor()
    cur.execute(query.select_indicators_table)
    for record in cur:
        print(record)
