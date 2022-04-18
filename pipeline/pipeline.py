import api
import postgres
import data_quality

def pipeline():
  api.trending()
  postgres.update_postgres()
  data_quality.fetch_postgres()

if __name__ == "__main__":
  pipeline()