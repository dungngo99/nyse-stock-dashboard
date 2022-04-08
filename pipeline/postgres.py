import configparser
import boto3
import logging
import pandas as pd
import psycopg2
import query
import io

logging.basicConfig(filename='pipeline/logs/postgres.log', level=logging.INFO)

config = configparser.ConfigParser()
config.read("config.cfg")

s3_client = boto3.client(
    "s3",
    region_name='us-west-2',
    aws_access_key_id=config['AWS']['aws_access_key_id'],
    aws_secret_access_key=config['AWS']['aws_secret_access_key']
)

bucket_name = config['S3']['bucket_name']


def load_s3():
    """Fetch S3 objects, convert data stream to dataframes, and store them locally as csv files  
    """
    def helper(key):
        key = key.strip("\n")
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", -1)

        if status == 200:
            logging.info(
                f"Successfully get an object from s3 @ s3://{bucket_name}/{key}")

            data = response['Body'].read().decode('utf-8').split()
            columns = data[0].split(",")
            rows = []
            for r in range(1, len(data)):
                rows.append(data[r].split(','))

            return columns, rows
        else:
            logging.error(
                f"Unsuccessfully get an object from s3. Status = {status}")

    with open("pipeline/logs/keys.txt", "r") as file:
        lines = file.readlines()
        stock_data = pd.DataFrame(columns=[
            "Timestamps",
            "Volume",
            "Low",
            "Open",
            "High",
            "Close",
            "Datetime",
            "symbol"
        ])

        meta_data = pd.DataFrame(columns=[
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
        ])

        for line in lines:
            meta_key, indicator_key = line.split(',')
            indicator_columns, indicator_rows = helper(indicator_key)
            meta_columns, meta_rows = helper(meta_key)

            buffer_df = pd.DataFrame(
                data=indicator_rows,
                columns=indicator_columns)
            stock_data = pd.concat([stock_data, buffer_df])

            meta_df = pd.DataFrame(
                data=meta_rows,
                columns=meta_columns)
            meta_data = pd.concat([meta_data, meta_df])

        stock_data.to_csv(
            "data/buffer/indicators.csv", index=False, header=False)
        meta_data.to_csv(
            "data/buffer/metadata.csv", index=False, header=False)


def update_postgres():
    """Load the local files and insert data into PosgreSQL tables
    """
    def helper(table, filename):
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

    conn = psycopg2.connect(host=config['Postgres']['host'],
                            dbname=config['Postgres']['dbName'],
                            user=config['Postgres']['user'],
                            password=config['Postgres']['password'],
                            port=config['Postgres']['port'])
    cur = conn.cursor()
    load_s3()

    try:
        helper("metadata", '/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/data/buffer/metadata.csv')
        helper("indicators", '/Users/ngodylan/Downloads/Data Engineering/Udacity D.E course/Capstone Project/nyse-stock-dashboard/data/buffer/indicators.csv')
    except ValueError:
        logging.error("Message error: value error")

    conn.close()


if __name__ == "__main__":
    update_postgres()
