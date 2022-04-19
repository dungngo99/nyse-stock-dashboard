select_latest_tickers = """
    SET TIMEZONE='America/Chicago';
    SELECT * 
    FROM trending 
    WHERE markettime >= (NOW()::timestamp - INTERVAL '1 day')
    AND markettime <= now()
    ORDER BY symbol;
"""

select_latest_charts = """
    SELECT low, open, high, close, datetime
    FROM indicators
    WHERE datetime >= (NOW()::timestamp - INTERVAL '1 day') 
    AND datetime <= NOW() AND symbol = '{ticker}'
"""

select_top_5_ticker_charts = """
    WITH q AS (
        SELECT low, open, high, close, datetime 
        FROM indicators
        WHERE symbol = '{ticker}'
        ORDER BY datetime DESC
        LIMIT 500
    )
    SELECT * FROM q
    ORDER BY datetime;
"""

select_top_5_ticker_profiles = """
    SELECT * FROM profile
    WHERE symbol = '{ticker}'
"""

select_news = """
    SELECT * 
    FROM news
    WHERE pubdate >= (NOW()::timestamp - INTERVAL '1 day')
    AND pubdate <= NOW()::timestamp
    LIMIT 50
"""