import json
import os
import glob
from collections import defaultdict
import pandas as pd
from datetime import datetime

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ParseIndicatorsOperator(BaseOperator):
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

        columns = ['Volume', 'Low', 'Open', 'High', "Close"]
        for file in files:
            dateId = file.split('/')[-1].split('.')[0]
            with open(file, 'r') as file:
                data = json.load(file)
                table['Timestamps'] = data['timestamp']
                indicators = data['indicators']['quote'][0]
                for column in columns:
                    table[column].extend(indicators[column.lower()])

        if table:
            df = pd.DataFrame(table)
            df['Datetime'] = df['Timestamps'].apply(lambda x: datetime.fromtimestamp(int(x)).strftime("%Y-%m-%d %H:%M:%S"))
            df['symbol'] = self.symbol
            df.to_csv(os.path.join(file_path, 'indicators_' + dateId + '.csv'), header=False, index=False)

if __name__ == "__main__":
    op = ParseIndicatorsOperator(task_id="test", dag=None, symbol='AMZN')
    op.execute()
    print("Successfully parsed data")
