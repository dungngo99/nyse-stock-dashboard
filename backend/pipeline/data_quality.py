import logging
import pipeline.query as query
import backend.config as config

logging.basicConfig(filename=config.data_quality_log_path, level=logging.INFO)

def main():
    cur = config.conn.cursor()
    cur.execute(query.test_select_indicators_table)
    for record in cur:
        logging.info(record)

if __name__ == "__main__":
    main()