import configparser
import boto3
import psycopg2
import os

# configs from config.cfg
configs = configparser.ConfigParser()
configs.read("./backend/config.cfg")

# api
api_base_url = "https://yh-finance.p.rapidapi.com/market/"
api_base_url_v2 = "https://yh-finance.p.rapidapi.com/stock/v2/"
api_base_url_news = "https://yh-finance.p.rapidapi.com/news/v2/"

api_headers = {
    'x-rapidapi-host': configs["RapidAPI"]['x-rapidapi-host'],
    'x-rapidapi-key': configs['RapidAPI']['x-rapidapi-key'],
    'Content-Type': "application/json"
}

# s3
s3_client = boto3.client(
    "s3",
    region_name='us-west-2',
    aws_access_key_id=configs['AWS']['aws_access_key_id'],
    aws_secret_access_key=configs['AWS']['aws_secret_access_key'])

s3_bucket_name = configs['S3']['bucket_name']
s3_trending_name = configs['S3']['trending_bucket_name']
s3_profile_name = configs['S3']['profile_bucket_name']
s3_news_name = configs['S3']['news_bucket_name']

# postgres connector
conn = psycopg2.connect(host=configs['Postgres']['host'],
                        dbname=configs['Postgres']['dbName'],
                        user=configs['Postgres']['user'],
                        password=configs['Postgres']['password'],
                        port=configs['Postgres']['port'])

# pipeline log paths
pipeline_log_base_path = "./backend/pipeline/logs"
data_quality_log_path = f"{pipeline_log_base_path}/output.log"
postgres_log_path = f"{pipeline_log_base_path}/postgres.log"
fetch_log_path = f"{pipeline_log_base_path}/fetch.log"

# endpoints log path
api_log_path = "./backend/endpoints/api.log"

# pipeline txt buffer
pipeline_buffer_base_path = './backend/pipeline/buffer'
indicators_txt_path = f"{pipeline_buffer_base_path}/indicators.txt"
metadata_txt_path = f"{pipeline_buffer_base_path}/meta.txt"
trending_txt_path = f"{pipeline_buffer_base_path}/trending.txt"
profile_txt_path = f"{pipeline_buffer_base_path}/profile.txt"
news_txt_path = f"{pipeline_buffer_base_path}/news.txt"

# pipeline csv buffer
root_path = "/tmp/postgres"
indicators_csv_path = os.path.abspath(f"{root_path}/indicators.csv")
metadata_csv_path = os.path.abspath(f"{root_path}/metadata.csv")
trending_csv_path = os.path.abspath(f"{root_path}/trending.csv")
profile_csv_path = os.path.abspath(f"{root_path}/profile.csv")
news_csv_path = os.path.abspath(f"{root_path}/news.csv")

s3_client = boto3.client(
    "s3",
    region_name="us-west-2",
    aws_access_key_id=configs["AWS"]["aws_access_key_id"],
    aws_secret_access_key=configs["AWS"]["aws_secret_access_key"],
)

bucket_name = configs["S3"]["bucket_name"]
trending_bucket_name = configs["S3"]["trending_bucket_name"]
profile_bucket_name = configs["S3"]["profile_bucket_name"]
news_bucket_name = configs["S3"]["news_bucket_name"]
