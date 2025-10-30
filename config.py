import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

URL_PATH = os.getenv(
  "SOURCE_URL",
  "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SRAG/2024/INFLUD24-26-06-2025.parquet"
)
DB_DIR = Path(os.getenv("DB_DIR", "data"))
DB_PATH = os.getenv("DB_PATH","data/srag.duckdb")
LOG_LEVEL="INFO"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

def validate_config():
  DB_DIR.mkdir(parents=True, exist_ok=True)
  return True

if __name__ == "__main__":
    validate_config()
    print("Configurações validadas com sucesso!")