from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.sql_queries import select_indicators_table, select_metadata_table

import os
import psycopg2
import configparser
import sys
sys.path.append('../..')

class CheckDataQualityOperator(BaseOperator):
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
        self.redshift_conn = f"""postgresql://{self.user_name}:{self.password}@{self.host}:{self.port}/{self.database}"""
        
    def execute(self):
        conn = psycopg2.connect(self.redshift_conn)
        cursor = conn.cursor()
        
        # Implement a non-empty check for 'indicators' table
        cursor.execute(select_indicators_table)
        indicators = cursor.fetchall()
        if len(indicators) < 1:
            raise Exception("Not passed non-empty table check")
            
        # Implement a non-duplicate-primary-key check for 'indicators' table
        # Table 'indicators' has a composite key (timestamp, symbol) for a primary key
        primaryKeys = set([])
        for row in indicators:
            if (row[0], row[7]) in primaryKeys:
                raise Exception("Not passed non-duplicate-primary-key table check")
            primaryKeys.add((row[0], row[7]))
        
        # Implement a non-empty check for 'metadata' table
        cursor.execute(select_metadata_table)
        metadata = cursor.fetchall()
        if len(metadata) < 1:
            raise Exception("Not passed non-empty table check")
        
        # Implement a non-duplicate-primary-key check for 'metadata' table
        # Table 'metadata' has a primary key (symbol)
        primaryKeys = set([])
        for row in metadata:
            if row[1] in primaryKeys:
                raise Exception("Not passed non-duplicate-primary-key table check")
            primaryKeys.add(row[1])
            
        cursor.close()
        conn.close()
