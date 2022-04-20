import psycopg2
import configparser
import query
import logging

config_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/config.cfg"
config = configparser.ConfigParser()
config.read(config_path)

log_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/pipeline/logs/output.log"
logging.basicConfig(filename=log_path,level=logging.INFO)

conn = psycopg2.connect(host=config['Postgres']['host'],
                        dbname=config['Postgres']['dbName'],
                        user=config['Postgres']['user'],
                        password=config['Postgres']['password'],
                        port=config['Postgres']['port'])

def fetch_postgres():
    """A data-quality that simply fetches queried data
    """
    cur = conn.cursor()
    cur.execute(query.select_metadata_table)
    for record in cur:
        logging.info(record)
