import requests
import pandas as pd
from datetime import datetime
import json
import configparser
import boto3
import os
import glob

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class UploadS3Operator(BaseOperator):
    @apply_defaults
    def __init__(self, symbol, config, rangeData="1d", interval="1m", *args, **kwargs):
        super().__init__(**kwargs)
        self.symbol = symbol
        self.config = config
        self.rangeData = rangeData
        self.interval = interval
        self.bucket = config['S3']['bucket_name']
        self.s3 = boto3.resource("s3",
                                region_name='us-west-2',
                                aws_access_key_id=self.config['AWS']['aws_access_key_id'],
                                aws_secret_access_key=self.config['AWS']['aws_secret_access_key'])

    # function to upload local files to S3 bucket
    def execute(self):
        # upload file to s3
        file_path = os.path.join("../../../", 'data', 'rapid_api', self.symbol, 'charts', f'{self.rangeData}_{self.interval}', '*.csv')
        files = glob.glob(file_path)

        for file in files:
            table, dateId = file.split('/')[-1].split('.')[0].split('_')
            key = os.path.join(self.symbol, table, self.rangeData + "_" + self.interval, dateId + ".csv")

            try:
                self.s3.Bucket(self.bucket).upload_file(file, key)
                print(f"Successfully uploaded an object @ {file} to S3 @ s3://{self.bucket}/{key}")
            except Exception as e:
                print(e)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("../../../config.cfg")

    op = UploadS3Operator(task_id="test", dag=None, symbol='AMZN', config=config)
    op.execute()
