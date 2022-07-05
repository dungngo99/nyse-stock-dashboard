import pipeline.fetch as fetch
import pipeline.postgres as postgres
import pipeline.data_quality as data_quality


def main():
    fetch.trending()
    fetch.news()
    postgres.update_postgres()
    data_quality.fetch_postgres()


if __name__ == "__main__":
    main()
