import logging
import os
from dotenv import load_dotenv
from pytrends.request import TrendReq
from etl.extract import extract_interest_over_time, extract_interest_by_region
from etl.load import load_interest_over_time, load_interest_by_region
from etl.transform import transform_interest_over_time, transform_interest_by_region
from utils import connect_to_postgres, load_config, send_email
from pytrends_etl import handle_etl


dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

config = load_config()  # <-- TERAZ zmienne środowiskowe są dostępne



LOG_LEVEL = config.get("log_level", "WARNING")
POSTGRES = config["postgres"]
EMAIL = config["email"]
PYTRENDS_CONFIGS = config.get("pytrends_configs", [])
PYTRENDS_TIMEOUT = tuple(config.get("pytrends_timeout", [20, 40]))

logger = logging.getLogger(__name__)

# Połącz z bazą
connection = connect_to_postgres(POSTGRES["host"], POSTGRES["db"], POSTGRES["user"], POSTGRES["password"])
cursor = connection.cursor()

# Inicjalizuj pytrends
pytrends = TrendReq(hl='en-US', tz=360, timeout=PYTRENDS_TIMEOUT, retries=5, backoff_factor=5)

for pytrends_config in config["pytrends_configs"]:
    if pytrends_config["topic"] != "programming_language":
        continue  # pomiń inne tematy

    print(f"\n=== Temat: {pytrends_config['topic']} ===")
    for data_type in ["interest_over_time", "interest_by_region"]:
        if data_type in pytrends_config:
            print(f"\n--- Typ: {data_type} ---")
            keyword = pytrends_config["keywords"][3]
            print(f"Przetwarzam: {keyword}")
            result = handle_etl(keyword, data_type, pytrends_config, pytrends, cursor, connection)
            print(f"Wynik: {result}")
            
connection.close()