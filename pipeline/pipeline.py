import api
import postgres
import data_quality

def pipeline():
  api.charts(["AAPL", "MSFT", "AMZN", "FB", "COIN"])
  api.trending()
  postgres.update_postgres()
  data_quality.fetch_postgres()

if __name__ == "__main__":
  pipeline()