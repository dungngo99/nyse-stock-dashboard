drop_meta_table = """
    DROP TABLE IF EXISTS metadata CASCADE;
"""

drop_indicators_table = """
    DROP TABLE IF EXISTS indicators CASCADE;
"""

create_meta_table = """
    CREATE TABLE IF NOT EXISTS metadata (
        currency TEXT NOT NULL,
        symbol TEXT PRIMARY KEY,
        instrumentType TEXT,
        firstTradeDate TEXT,
        exchangeTimezonName TEXT,
        timezone TEXT,
        trade_period TEXT,
        range TEXT,
        interval TEXT,
        start_date TEXT
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
        symbol TEXT NOT NULL REFERENCES metadata(symbol),
        PRIMARY KEY (timestamps, symbol)
    );
"""

create_trending_table = """
    CREATE TABLE IF NOT EXISTS trending (
        region TEXT,
        quoteType TEXT,
        marketChangePercent DECIMAL,
        firstTradeDate TIMESTAMP NOT NULL,
        marketTime TIMESTAMP NOT NULL,
        marketPrice DECIMAL,
        exchange TEXT,
        shortName TEXT,
        symbol TEXT PRIMARY KEY
    );
"""

update_meta_table = """
    CREATE TEMP TABLE buffer AS SELECT * FROM {table} LIMIT 0;
    
    COPY buffer 
    FROM '{filename}'
    (FORMAT csv);
    
    INSERT INTO {table}
    SELECT *
    FROM buffer ON CONFLICT (symbol) 
    DO UPDATE
    SET 
        currency = EXCLUDED.currency,
        symbol = EXCLUDED.symbol,
        instrumentType = EXCLUDED.instrumentType,
        firstTradeDate = EXCLUDED.firstTradeDate,
        exchangeTimezonName = EXCLUDED.exchangeTimezonName,
        timezone = EXCLUDED.timezone,
        trade_period = EXCLUDED.trade_period,
        range = EXCLUDED.range,
        interval = EXCLUDED.interval,
        start_date = EXCLUDED.start_date;

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

update_trending_table = """
    CREATE TEMP TABLE buffer AS SELECT * FROM {table} LIMIT 0;
    
    COPY buffer
    FROM '{filename}'
    DELIMITER ','
    CSV HEADER;
    
    INSERT INTO {table}
    SELECT *
    FROM buffer ON CONFLICT (symbol)
    DO UPDATE
    SET 
        region = EXCLUDED.region,
        quoteType = EXCLUDED.quoteType,
        marketChangePercent = EXCLUDED.marketChangePercent,
        firstTradeDate = EXCLUDED.firstTradeDate,
        marketTime = EXCLUDED.marketTime,
        marketPrice = EXCLUDED.marketPrice,
        exchange = EXCLUDED.exchange,
        shortName = EXCLUDED.shortName;
        
    DROP TABLE buffer;
"""

select_indicators_table = """
    SELECT * FROM indicators WHERE symbol='AAPL' LIMIT 10;
"""

select_metadata_table = """
    SELECT * FROM metadata WHERE symbol='AAPL';
"""
