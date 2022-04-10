select_latest_tickers = """
    SET TIMEZONE='America/Chicago';
    SELECT * 
    FROM trending 
    WHERE markettime >= (NOW()::timestamp - INTERVAL '1 day')
    AND markettime <= now()
    ORDER BY symbol;
"""
