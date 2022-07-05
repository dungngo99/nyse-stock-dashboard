import logging
import pandas as pd
import pipeline.query as query
import backend.config as config

logging.basicConfig(filename=config.postgres_log_path, level=logging.INFO)

indicators_columns = [
    "Timestamps",
    "Volume",
    "Low",
    "Open",
    "High",
    "Close",
    "Datetime",
    "symbol",
]
metadata_columns = [
    "currency",
    "symbol",
    "instrumentType",
    "firstTradeDate",
    "exchangeTimezoneName",
    "timezone",
    "trade_period",
    "range",
    "interval",
    "start_date",
]
trending_columns = [
    "region",
    "quoteType",
    "marketChangePercent",
    "firstTradeDate",
    "marketTime",
    "marketPrice",
    "exchange",
    "shortName",
    "symbol",
]
profile_columns = [
    "openPrice",
    "exchangeName",
    "marketTime",
    "name",
    "currency",
    "marketCap",
    "quoteType",
    "exchangeTimezoneName",
    "beta",
    "yield",
    "dividendRate",
    "strikePrice",
    "ask",
    "sector",
    "fullTimeEmployees",
    "longBusinessSummary",
    "city",
    "country",
    "website",
    "industry",
]
news_columns = [
    "id",
    "contentType",
    "title",
    "pubDate",
    "thumbnailUrl",
    "thumbnailWidth",
    "thumbnailHeight",
    "thumbailTag",
    "Url",
    "provider",
]


def load_s3(columns, bucket_name, txt_path, csv_path):
    """Fetch S3 objects, convert data stream to dataframes, and store them locally as csv files"""
    with open(txt_path, "r") as file:
        lines = file.readlines()
        data = pd.DataFrame(columns=columns)

        for key in lines:
            response = config.s3_client.get_object(Bucket=bucket_name,
                                                   Key=key.strip("\n"))
            status = response.get("ResponseMetadata",
                                  {}).get("HTTPStatusCode", -1)

            if status == 200:
                logging.info(
                    f"Successfully get an object from s3 @ s3://{bucket_name}/{key}"
                )
                buffer_df = pd.read_csv(response["Body"])
                data = pd.concat([data, buffer_df])
            else:
                logging.error(
                    f"Unsuccessfully get an object from s3. Status = {status}")

        data.to_csv(csv_path, index=False, header=False)


def update_postgres(table, filename):
    cur = config.conn.cursor()

    if table == "metadata":
        cur.execute(query.create_meta_table)
        cur.execute(
            query.update_meta_table.format(filename=filename, table=table))
        config.conn.commit()

    elif table == "indicators":
        cur.execute(query.create_indicators_table)
        cur.execute(
            query.update_indicators_table.format(filename=filename,
                                                 table=table))
        config.conn.commit()

    elif table == "trending":
        cur.execute(query.create_trending_table)
        cur.execute(
            query.update_trending_table.format(filename=filename, table=table))
        config.conn.commit()

    elif table == "profile":
        cur.execute(query.create_profile_table)
        cur.execute(
            query.update_profile_table.format(filename=filename, table=table))
        config.conn.commit()

    elif table == "news":
        cur.execute(query.create_news_table)
        cur.execute(
            query.update_news_table.format(filename=filename, table=table))
        config.conn.commit()

    logging.info(
        f"Successfully uploaded file {filename} to relation {table} in PostgreSQL"
    )


def metadata():
    load_s3(metadata_columns, config.bucket_name, config.metadata_txt_path,
            config.metadata_csv_path)
    update_postgres("metadata", config.metadata_csv_path)


def indicators():
    load_s3(indicators_columns, config.bucket_name, config.indicators_txt_path,
            config.indicators_csv_path)
    update_postgres("indicators", config.indicators_csv_path)


def trending():
    load_s3(trending_columns, config.trending_bucket_name,
            config.trending_txt_path, config.trending_csv_path)
    update_postgres("trending", config.trending_csv_path)


def profile():
    load_s3(profile_columns, config.profile_bucket_name,
            config.profile_txt_path, config.profile_csv_path)
    update_postgres("profile", config.profile_csv_path)


def news():
    load_s3(news_columns, config.news_bucket_name, config.news_txt_path,
            config.news_csv_path)
    update_postgres("news", config.news_csv_path)


def main():
    """Load the local files and insert data into PosgreSQL tables"""
    try:
        metadata()
        indicators()
        trending()
        profile()
        news()
    except ValueError:
        logging.error("Message error: value error")
    finally:
        config.conn.close()


if __name__ == "__main__":
    main()
