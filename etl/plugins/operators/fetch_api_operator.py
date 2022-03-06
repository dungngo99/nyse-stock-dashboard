import requests
import json
import configparser
import boto3
import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from utilities import create_metadata, create_indicators, upload_object


class FetchAPIOperator(BaseOperator):
    LOGGER = logging.getLogger(__name__)

    @apply_defaults
    def __init__(self, config, **kwargs):
        super().__init__(**kwargs)
        self.base_url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/market/get-charts"

        self.query_string = {
            "region": "US",
            "lang": "en",
            "symbol": config['env']['symbol'],
            "range": config['env']['rangeData'],
            "interval": config['env']['interval']
        }

        self.headers = {
            'x-rapidapi-host': config["RapidAPI"]['x-rapidapi-host'],
            'x-rapidapi-key': config['RapidAPI']['x-rapidapi-key'],
            'Content-Type': "application/json"
        }

        self.s3_client = boto3.client(
            "s3",
            region_name='us-west-2',
            aws_access_key_id=config['AWS']['aws_access_key_id'],
            aws_secret_access_key=config['AWS']['aws_secret_access_key']
        )

        self.bucket_name = config['S3']['bucket_name']

    def execute(self):
        response = requests.request(
            "GET", self.base_url, headers=self.headers, params=self.query_string
        )

        if response.ok:
            data = json.loads(response.content)
            meta_df = create_metadata(data)
            df = create_indicators(data)
            mes = upload_object(
                (self.s3_client, self.bucket_name),
                (meta_df, df), "indicators"
            )

            if type(mes) == "str":
                self.LOGGER.error(mes)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("../../../config.cfg")

    op = FetchAPIOperator(task_id="test", dag=None, config=config)
    op.execute()
    print("Successfully uploaded data to S3")
