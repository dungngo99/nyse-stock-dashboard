import json
import os
import glob
from collections import defaultdict
import pandas as pd
from datetime import datetime

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ParseMetaDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, symbol, rangeData="1d", interval="1m", *args, **kwargs):
        super().__init__(**kwargs)
        self.symbol = symbol
        self.rangeData = rangeData
        self.interval = interval

    def execute(self):
        file_path = os.path.join("../../../", 'data', 'rapid_api', self.symbol, 'charts', f'{self.rangeData}_{self.interval}')
        files = glob.glob(os.path.join(file_path, '*.json'))
        table = defaultdict(list)

        for file in files:
            dateId = file.split('/')[-1].split('.')[0]
            with open(file, 'r') as file:
                data = json.load(file)
                metadata = data['meta']

                table = {
                    "currency": metadata['currency'],
                    "symbol": metadata['symbol'],
                    "instrumentType": metadata['instrumentType'],
                    "firstTradeDate": datetime.fromtimestamp(metadata['firstTradeDate']).strftime("%Y-%m-%d %H:%M:%S"),
                    "exchangeTimezoneName": metadata["exchangeTimezoneName"],
                    'timezone': metadata['timezone'],
                    'trade_period': self.get_trading_period(metadata['tradingPeriods']),
                    'range': metadata['range'],
                    'interval': metadata['dataGranularity'],
                    'start_date': min(data['timestamp'])
                }

        if table:
            df = pd.DataFrame(table)
            df.to_csv(os.path.join(file_path, 'metadata_' + dateId + '.csv'), header=False, index=False)

    def get_trading_period(self, trade_period):
        trading_periods = set([])
        for period in trade_period:
            start = datetime.fromtimestamp(int(period[0]['start'])).strftime("%H:%M:%S")
            end = datetime.fromtimestamp(int(period[0]['end'])).strftime("%H:%M:%S")
            date = f"{start} - {end}"
            trading_periods.add(date)
        return list(trading_periods)


if __name__ == "__main__":
    op = ParseMetaDataOperator(task_id="test", dag=None, symbol='AMZN')
    op.execute()
    print("Successfully parsed data")
