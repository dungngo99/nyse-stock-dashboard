create_indicators_table = """
    CREATE TABLE IF NOT EXISTS metadata (
        currency TEXT NOT NULL,
        symbol TEXT PRIMARY KEY,
        instrumentType TEXT NOT NULL,
        firstTradeDate TEXT NOT NULL,
        exchangeTimezonName TEXT NOT NULL,
        timezone TEXT NOT NULL,
        trade_period TEXT NOT NULL,
        range TEXT NOT NULL,
        interval TEXT NOT NULL,
        start_date TEXT NOT NULL
    );
"""

create_metadata_table = """
    CREATE TABLE IF NOT EXISTS indicators (
        timestamps BIGINT NOT NULL,
        volume DECIMAL NOT NULL,
        low DECIMAL NOT NULL,
        open_ DECIMAL NOT NULL,
        high DECIMAL NOT NULL,
        close DECIMAL NOT NULL,
        datetime TIMESTAMP NOT NULL,
        symbol TEXT NOT NULL REFERENCES metadata(symbol),
        PRIMARY KEY (timestamps, symbol)
    );
"""

copy_metadata_to_redshift = """
    TRUNCATE metadata;
    COPY metadata FROM 's3://{bucket_name}/{key_path_meta}'
    CREDENTIALS 'aws_iam_role={role_arn}'
    DELIMITER ',';
"""

copy_indicators_to_redshift = """
    TRUNCATE indicators;
    COPY indicators FROM 's3://{bucket_name}/{key_path_indicators}'
    CREDENTIALS 'aws_iam_role={role_arn}'
    DELIMITER ',';
"""

select_indicators_table = """
    SELECT * FROM indicators;
"""

select_metadata_table = """
    SELECT * FROM metadata;
"""