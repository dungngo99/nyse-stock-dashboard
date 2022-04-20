import configparser
from sys import meta_path
import boto3
import logging
import pandas as pd
import psycopg2
import query

log_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/pipeline/logs/postgres.log"
config_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/config.cfg"

metadata_txt_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/pipeline/buffer/meta.txt"
indicators_txt_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/pipeline/buffer/indicators.txt"
trending_txt_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/pipeline/buffer/trending.txt"
profile_txt_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/pipeline/buffer/profile.txt"
news_txt_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/pipeline/buffer/news.txt"

indicators_csv_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/pipeline/buffer/indicators.csv"
metadata_csv_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/pipeline/buffer/metadata.csv"
trending_csv_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/pipeline/buffer/trending.csv"
profile_csv_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/pipeline/buffer/profile.csv"
news_csv_path = "/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/backend/pipeline/buffer/news.csv"

logging.basicConfig(filename=log_path, level=logging.INFO)
config = configparser.ConfigParser()
config.read(config_path)

s3_client = boto3.client(
    "s3",
    region_name='us-west-2',
    aws_access_key_id=config['AWS']['aws_access_key_id'],
    aws_secret_access_key=config['AWS']['aws_secret_access_key']
)

bucket_name = config['S3']['bucket_name']
trending_bucket_name = config['S3']['trending_bucket_name']
profile_bucket_name = config['S3']['profile_bucket_name']
news_bucket_name = config['S3']['news_bucket_name']

indicators_columns = [
    "Timestamps", 
    "Volume", 
    "Low", 
    "Open", 
    "High", 
    "Close", 
    "Datetime", 
    "symbol"
]
metadata_columns = [
    "currency", 
    "symbol",
    "instrumentType",
    "firstTradeDate",
    "exchangeTimezoneName",
    'timezone',
    'trade_period',
    'range',
    'interval',
    'start_date'
]
trending_columns = [
    "region",
    "quoteType",
    "marketChangePercent",
    "firstTradeDate",
    'marketTime',
    "marketPrice",
    "exchange",
    "shortName",
    "symbol"
]
profile_columns = [
    'openPrice',
    'exchangeName',
    'marketTime',
    'name',
    'currency',
    'marketCap',
    'quoteType',
    'exchangeTimezoneName',
    'beta',
    'yield',
    'dividendRate',
    'strikePrice',
    'ask',
    'sector',
    'fullTimeEmployees',
    'longBusinessSummary',
    'city',
    'country',
    'website',
    'industry'
]
news_columns = [
    'id',
    'contentType',
    'title',
    'pubDate',
    'thumbnailUrl',
    'thumbnailWidth',
    'thumbnailHeight',
    'thumbailTag',
    'Url',
    'provider'
]

sql_queries = {
    
}

conn = psycopg2.connect(host=config['Postgres']['host'],
                        dbname=config['Postgres']['dbName'],
                        user=config['Postgres']['user'],
                        password=config['Postgres']['password'],
                        port=config['Postgres']['port'])

def get_s3(key, bucket_name):
    key = key.strip("\n")
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", -1)

    if status == 200:
        logging.info(f"Successfully get an object from s3 @ s3://{bucket_name}/{key}")
        return pd.read_csv(response['Body'])

    else:
        logging.error(f"Unsuccessfully get an object from s3. Status = {status}")


def load_s3(columns, bucket_name, txt_path, csv_path):
    """Fetch S3 objects, convert data stream to dataframes, and store them locally as csv files  
    """
    with open(txt_path, "r") as file:
        lines = file.readlines()
        data = pd.DataFrame(columns=columns)
        
        for line in lines:
            buffer_df = get_s3(line, bucket_name)
            data = pd.concat([data, buffer_df])

        data.to_csv(csv_path, index=False, header=False)
        
def update_postgres():
    """Load the local files and insert data into PosgreSQL tables
    """
    def helper(table, filename):
        cur = conn.cursor()
        
        if table == 'metadata':
            cur.execute(query.create_meta_table)
            cur.execute(query.update_meta_table.format(
                filename=filename, table=table))
            conn.commit()
        elif table == 'indicators':
            cur.execute(query.create_indicators_table)
            cur.execute(query.update_indicators_table.format(
                filename=filename, table=table))
            conn.commit()
        elif table == 'trending':
            cur.execute(query.create_trending_table)
            cur.execute(query.update_trending_table.format(
                filename=filename, table=table))
            conn.commit()
        elif table == 'profile':
            cur.execute(query.create_profile_table)
            cur.execute(query.update_profile_table.format(
                filename=filename, table=table))
            conn.commit()
        elif table == "news":
            cur.execute(query.create_news_table)
            cur.execute(query.update_news_table.format(
                filename=filename, table=table))
            conn.commit()

    try:
        load_s3(metadata_columns, bucket_name, metadata_txt_path, metadata_csv_path)
        load_s3(indicators_columns, bucket_name, indicators_txt_path, indicators_csv_path)
        load_s3(trending_columns, trending_bucket_name, trending_txt_path, trending_csv_path)
        load_s3(profile_columns, profile_bucket_name, profile_txt_path, profile_csv_path)
        load_s3(news_columns, news_bucket_name, news_txt_path, news_csv_path)
            
        helper("metadata", metadata_csv_path)
        helper("indicators", indicators_csv_path)
        helper("trending", trending_csv_path)
        helper("profile", profile_csv_path)
        helper("news", news_csv_path)
    except ValueError:
        logging.error("Message error: value error")
    finally:
        conn.close()


if __name__ == "__main__":
    update_postgres()
