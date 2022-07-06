select_latest_ticker = """
    SET TIMEZONE='America/Chicago';
    SELECT * 
    FROM profile 
    WHERE market_time >= (NOW()::timestamp - INTERVAL '1 day')
    AND markettime <= now()
    ORDER BY symbol;
"""

select_latest_chart = """
    SELECT low, open, high, close, datetime
    FROM indicators
    WHERE datetime >= (NOW()::timestamp - INTERVAL '1 day') 
    AND datetime <= NOW() 
    AND symbol = '{ticker}'
    ORDER BY datetime
"""

select_ticker_profile = """
    SELECT * FROM profile
    WHERE symbol = '{ticker}'
"""

select_news = """
    SELECT * 
    FROM news
    WHERE pub_date >= (NOW()::timestamp - INTERVAL '1 day')
    AND pub_date <= NOW()::timestamp
    LIMIT 50
"""