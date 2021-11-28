import sys
sys.path.append('../..')

import configparser
import psycopg2
import os
import boto3
from collections import defaultdict

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.sql_queries import create_indicators_table, create_metadata_table, copy_metadata_to_redshift, copy_indicators_to_redshift

class MigrateS3ToRedshiftOperator(BaseOperator):
    @apply_defaults
    def __init__(self, symbol, config, rangeData="1d", interval="1m", *args, **kwargs):
        super().__init__(**kwargs)
        self.symbol = symbol
        self.config = config
        self.rangeData = rangeData
        self.interval = interval
        self.user_name = self.config['Redshift']['user_name']
        self.password = self.config['Redshift']['password']
        self.host = self.config['Redshift']['host']
        self.port = self.config['Redshift']['port']
        self.database = self.config['Redshift']['database']
        self.redshift_conn =  f"""postgresql://{self.user_name}:{self.password}@{self.host}:{self.port}/{self.database}"""
        self.reshift = redshift = boto3.client('redshift',
                                                region_name = 'us-west-2',
                                                aws_access_key_id = config['AWS']['aws_access_key_id'],
                                                aws_secret_access_key = config['AWS']['aws_secret_access_key'])

    # function to upload local files to S3 bucket
    def execute(self):
        try:
            conn = psycopg2.connect(self.redshift_conn)
            cursor = conn.cursor()
            cursor.execute(create_indicators_table)
            print("Created indicators table")
            cursor.execute(create_metadata_table)
            print("Created metadata table")

            cursor.execute(copy_metadata_to_redshift.format(
                bucket_name =self.config['S3']['bucket_name'],
                key_path_meta = os.path.join(self.symbol, 'metadata', self.rangeData + "_" + self.interval),
                role_arn = self.config['Redshift']['role_arn']
            ))
            print("Migrated metadata table")
            cursor.execute(copy_indicators_to_redshift.format(
                bucket_name = self.config['S3']['bucket_name'],
                key_path_indicators = os.path.join(self.symbol, 'indicators', self.rangeData + "_" + self.interval),
                role_arn = self.config['Redshift']['role_arn']
            ))
            print("Migrated indicators table")

            conn.commit()
            cursor.close(); conn.close()
        except Exception as e:
            print(e)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("../../../config.cfg")

    op = MigrateS3ToRedshiftOperator(task_id="test", dag=None, symbol='AMZN', config=config)
    op.execute()
