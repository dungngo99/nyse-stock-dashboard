create_profile_table = """
    CREATE TABLE IF NOT EXISTS profile (
        region TEXT,
        market_change_percent DECIMAL,
        market_price DECIMAL,
        symbol TEXT PRIMARY KEY,
        currency TEXT,
        type TEXT,
        first_trade_date TEXT,
        exchange_timezone TEXT,
        trade_period TEXT,
        start_trading_datetime TEXT,
        exchange_name TEXT,
        market_time TEXT,
        name TEXT,
        market_cap TEXT,
        beta TEXT,
        yield TEXT,
        dividend_rate TEXT,
        strike_price TEXT,
        ask TEXT,
        sector TEXT,
        fulltime_employees NUMERIC,
        city TEXT,
        country TEXT,
        website TEXT,
        industry TEXT
    );
"""

create_indicators_table = """
    CREATE TABLE IF NOT EXISTS indicators (
        timestamps BIGINT NOT NULL,
        volume DECIMAL,
        low DECIMAL,
        open DECIMAL,
        high DECIMAL,
        close DECIMAL,
        datetime TIMESTAMP NOT NULL,
        symbol TEXT NOT NULL REFERENCES profile(symbol),
        PRIMARY KEY (timestamps, symbol)
    );
"""

create_news_table = """
    CREATE TABLE IF NOT EXISTS news (
        id TEXT PRIMARY KEY,
        content_type TEXT,
        title TEXT,
        pub_date TIMESTAMP NOT NULL,
        thumbnail_url TEXT,
        thumbnail_width INTEGER,
        thumbnail_height INTEGER,
        thumbail_tag TEXT,
        url TEXT,
        provider TEXT
    )
"""

update_profile_table = """
    CREATE TEMP TABLE buffer AS SELECT * FROM {table} LIMIT 0;
    
    COPY buffer 
    FROM '{filename}'
    (FORMAT csv);
    
    INSERT INTO {table}
    SELECT *
    FROM buffer ON CONFLICT (symbol) 
    DO UPDATE
    SET 
        symbol = EXCLUDED.symbol,
        name = EXCLUDED.name,
        currency = EXCLUDED.currency,
        first_trade_date = EXCLUDED.first_trade_date,
        exchange_timezone = EXCLUDED.exchange_timezone,
        trade_period = EXCLUDED.trade_period,
        type = EXCLUDED.type,
        start_trading_datetime = EXCLUDED.start_trading_datetime,
        market_time = EXCLUDED.market_time,
        market_cap = EXCLUDED.market_cap,
        beta = EXCLUDED.beta,
        yield = EXCLUDED.yield,
        dividend_rate = EXCLUDED.dividend_rate,
        strike_price = EXCLUDED.strike_price,
        ask = EXCLUDED.ask,
        sector = EXCLUDED.sector,
        fulltime_employees = EXCLUDED.fulltime_employees,
        city = EXCLUDED.city,
        country = EXCLUDED.country,
        website = EXCLUDED.website,
        industry = EXCLUDED.industry,
        market_price = EXCLUDED.market_price,
        market_change_percent = EXCLUDED.market_change_percent,
        region = EXCLUDED.region;

    DROP TABLE buffer;
"""

update_indicators_table = """
    CREATE TEMP TABLE buffer AS SELECT * FROM {table} LIMIT 0;  
    
    COPY buffer
    FROM '{filename}'
    DELIMITER ',' 
    CSV HEADER;
    
    INSERT INTO {table}
    SELECT *
    FROM buffer ON CONFLICT (timestamps, symbol)
    DO NOTHING;
    
    DROP TABLE buffer; 
"""

update_news_table = """
    CREATE TEMP TABLE buffer AS SELECT * FROM {table} LIMIT 0;
    
    COPY buffer
    FROM '{filename}'
    DELIMITER ','
    CSV HEADER;
    
    INSERT INTO {table}
    SELECT *
    FROM buffer ON CONFLICT (id)
    DO UPDATE
    SET 
        content_type = EXCLUDED.content_type,
        title = EXCLUDED.title,
        pub_date = EXCLUDED.pub_date,
        thumbnail_url = EXCLUDED.thumbnail_url,
        thumbnail_width = EXCLUDED.thumbnail_width,
        thumbnail_height = EXCLUDED.thumbnail_height,
        thumbail_tag = EXCLUDED.thumbail_tag,
        url = EXCLUDED.url,
        provider = EXCLUDED.provider;

    DROP TABLE buffer;
"""

test_select_indicators_table = """
    SELECT * FROM indicators WHERE symbol='AAPL' LIMIT 5;
"""
