import requests
import json
import configparser
import os
from datetime import datetime

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class FetchAPIOperator(BaseOperator):
    @apply_defaults
    def __init__(self, symbol, config, rangeData="1d", interval="1m", *args, **kwargs):
        super().__init__(**kwargs)
        self.symbol = symbol
        self.config = config
        self.rangeData = rangeData
        self.interval = interval

    def execute(self):
        base_url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/market/get-charts"
        querystring = {
            "region": "US",
            "lang": "en",
            "symbol": self.symbol,
            "range": self.rangeData,
            "interval": self.interval
        }
        headers = {
            'x-rapidapi-host': self.config["RapidAPI"]['x-rapidapi-host'],
            'x-rapidapi-key': self.config['RapidAPI']['x-rapidapi-key'],
            'Content-Type': "application/json"
        }
        response = requests.request(
            "GET", base_url, headers=headers, params=querystring)
        if response.ok:
            data = json.loads(response.content)
            res = data['chart']['result'][0]

            dateId = datetime.fromtimestamp(min(res['timestamp'])).strftime("%m-%d-%Y")
            path = os.path.join("../../../", 'data', 'rapid_api', self.symbol, 'charts', f'{self.rangeData}_{self.interval}')
            file_path = os.path.join(path, f"{dateId}.json")
            print("File path:", file_path)

            if not os.path.exists(path):
                os.makedirs(path)
            with open(file_path, 'w') as file:
                json.dump(res, file)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("../../../config.cfg")

    op = FetchAPIOperator(task_id="test", dag=None, symbol='AMZN', config=config)
    op.execute()
    print("Successfully fetched data")
