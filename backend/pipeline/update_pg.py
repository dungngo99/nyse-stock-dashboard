import logging

import backend.config as config

import pipeline.query as query


def update_table(table, filename, create_table_query, update_table_query):
    cur = config.conn.cursor()
    cur.execute(create_table_query)
    config.conn.commit()

    cur.execute(update_table_query.format(filename=filename, table=table))
    config.conn.commit()
    logging.info(f"Uploaded file {filename} to table {table} in PostgreSQL")


def update_postgres():
    update_table(
        "profile",
        config.profile_csv_path,
        query.create_profile_table,
        query.update_profile_table
    )

    update_table(
        "indicators",
        config.indicators_csv_path,
        query.create_indicators_table,
        query.update_indicators_table
    )

    update_table(
        "news", 
        config.news_csv_path, 
        query.create_news_table, 
        query.update_news_table
    )


if __name__ == "__main__":
    update_postgres()