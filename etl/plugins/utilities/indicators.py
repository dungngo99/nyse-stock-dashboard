import pandas as pd
from datetime import datetime


def create_indicators(response):
    metadata = response['meta']
    timestamps = response['timestamp']
    indicators = response['indicators']['quote'][0]

    table = {
        "Timestamps": timestamps,
        "Volume": indicators['volume'],
        "Low": indicators["low"],
        "Open": indicators["open"],
        "High": indicators["high"],
        "Close": indicators["close"]
    }

    tss = pd.Series(timestamps).apply(
        lambda x: datetime.fromtimestamp(int(x)).strftime("%Y-%m-%d %H:%M:%S"))

    df_indicators = pd.DataFrame(table)
    df_indicators['Datetime'] = tss
    df_indicators['symbol'] = metadata['symbol']

    return df_indicators
