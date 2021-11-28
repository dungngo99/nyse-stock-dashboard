import configparser
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from operators import (CheckDataQualityOperator,
                        FetchAPIOperator,
                        MigrateS3ToRedshiftOperator,
                        ParseIndicatorsOperator,
                        ParseMetaDataOperator,
                        UploadS3Operator)

# Create a config object
config = configparser.ConfigParser()
config.read("../../config.cfg")

# add default arguments
default_args = {
    'owner': 'dngo',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'on_failure_callback': logging.error("Failed to execute 'udac_example_dag' DAG")
}

# create the main DAG
dag = DAG('nyse-stock',
          default_args=default_args,
          description='an end-to-end ETL Pipeline from feching API to S3 to Redshift',
          schedule_interval='0 * * * *'
          )

# create a dummy operator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# fetch stock data from RapidAPI
fetch_stock_data_task = FetchAPIOperator(
    task_id="fetch_stock_data_task",
    dag=dag,
    config =  config,
    symbol = config['env']['symbol'],
    rangeData = config['env']['rangeData'],
    interval = config['env']['interval']
)

# process indicators table
process_indicators_table_task = ParseIndicatorsOperator(
    task_id="process_indicators_table_task",
    dag=dag,
    symbol = config['env']['symbol'],
    rangeData = config['env']['rangeData'],
    interval = config['env']['interval']
)

# process metadata table
process_metadata_table_task = ParseMetaDataOperator(
    task_id="process_metadata_table_task",
    dag=dag,
    symbol = config['env']['symbol'],
    rangeData = config['env']['rangeData'],
    interval = config['env']['interval']
)

# upload metadata table to S3
upload_metadata_to_S3_task = UploadS3Operator(
    task_id="upload_metadata_S3_task",
    dag=dag,
    config =  config,
    symbol = config['env']['symbol'],
    rangeData = config['env']['rangeData'],
    interval = config['env']['interval']
)

# migrate S3 to redshift
migrate_S3_to_redshift_task = MigrateS3ToRedshiftOperator(
    task_id="migrate_S3_to_redshift_task",
    dag=dag,
    config =  config,
    symbol = config['env']['symbol'],
    rangeData = config['env']['rangeData'],
    interval = config['env']['interval']
)

# include data quality
data_quality_check_task = CheckDataQualityOperator(
    task_id="data_quality_check_task",
    dag=dag
)

# create a dummy operator
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Add the dependencies
start_operator >> fetch_stock_data_task

fetch_stock_data_task >> [process_indicators_table_task, process_metadata_table_task]

[process_indicators_table_task, process_metadata_table_task] >> upload_to_S3_task

upload_to_S3_task >> migrate_S3_to_redshift_task

migrate_S3_to_redshift_task >> data_quality_check_task

data_quality_check_task >> end_operator
