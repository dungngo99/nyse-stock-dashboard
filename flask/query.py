select_latest_tickers = """
    SET TIMEZONE='America/Chicago';
    SELECT * 
    FROM trending 
    WHERE markettime >= (NOW()::timestamp - INTERVAL '1 day')
    AND markettime <= now()
    ORDER BY symbol;
"""

select_latest_ticker_charts = """
    SELECT low, open, high, close, datetime FROM indicators
    WHERE datetime >= (NOW()::timestamp - INTERVAL '1 day') 
    AND datetime <= NOW() AND symbol = '{ticker}'
"""