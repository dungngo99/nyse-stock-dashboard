import pandas as pd
from datetime import datetime


def create_metadata(response):
    metadata = response['meta']
    timestamps = response['timestamp']

    def get_trading_period(trade_period):
        trading_periods = set([])
        for period in trade_period:
            start = datetime.fromtimestamp(
                int(period[0]['start'])).strftime("%H:%M:%S")
            end = datetime.fromtimestamp(
                int(period[0]['end'])).strftime("%H:%M:%S")
            date = f"{start} - {end}"
            trading_periods.add(date)
        return list(trading_periods)

    tss = pd.Series(timestamps).apply(
        lambda x: datetime.fromtimestamp(int(x)).strftime("%Y-%m-%d %H:%M:%S"))

    impt_metadata = {
        "currency": metadata['currency'],
        "symbol": metadata['symbol'],
        "instrumentType": metadata['instrumentType'],
        "firstTradeDate": datetime.fromtimestamp(metadata['firstTradeDate']).strftime("%Y-%m-%d %H:%M:%S"),
        "exchangeTimezoneName": metadata["exchangeTimezoneName"],
        'timezone': metadata['timezone'],
        'trade_period': get_trading_period(metadata['tradingPeriods']),
        'range': metadata['range'],
        'interval': metadata['dataGranularity'],
        'start_date': tss.min()
    }

    return pd.DataFrame(impt_metadata)
