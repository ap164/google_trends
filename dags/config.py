import os
from dotenv import load_dotenv

# Załaduj zmienne środowiskowe z pliku .env
load_dotenv(dotenv_path="/Users/aniaprus/airflow-docker/.env")

# Konfiguracja logowania
LOG_LEVEL = "WARNING" 
# Konfiguracja PostgreSQL (dane pobierane z .env)
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Konfiguracja e-mail (dane pobierane z .env)
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_EMAIL_PASSWORD = os.getenv("SENDER_EMAIL_PASSWORD")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")

# Konfiguracja dla różnych procesów
PYTRENDS_CONFIGS = [
    {
        "keywords": ["python", "java"],
        "timeframe": "now 1-d",  # Codziennie
        "category": 0,
        "geo": "",
        "gprop": "youtube",
        "schedule_interval": "@daily"
    },
    {
        "keywords": ["remote work", "4 day work week", "benefits", "flexible hours", "medical insurance"],
        "timeframe": "now 1-H",  # Co tydzień
        "category": 0,
        "geo": "",
        "gprop": "",
        "schedule_interval": "@daily"
    },
    {
        "keywords": ["aaa", "aaa", "aaaa", "dddd ", "eeee"],
        "timeframe": "now 1-H",  # Co tydzień
        "category": 0,
        "geo": "",
        "gprop": "",
        "schedule_interval": "@daily"
    },
    {
        "keywords": ["1", "2", "3", "4"],
        "timeframe": "now 1-H",  # Co tydzień
        "category": 0,
        "geo": "",
        "gprop": "",
        "schedule_interval": "@daily"
    },
    {
        "keywords": ["remote ", " work week", "533", "flexible ", " insurance"],
        "timeframe": "now 1-H",  # Co tydzień
        "category": 0,
        "geo": "",
        "gprop": "",
        "schedule_interval": "@daily"
    },
    {
        "keywords": ["ai","chat gpt","copilot"],
        "timeframe": "now 1-d",  # Co godzinę
        "category": 0,
        "geo": "",
        "gprop": "",
        "schedule_interval": "@hourly"
    },
    {
        "keywords": ["jira","confluence","agile"],
        "timeframe": "now 1-H",  # Co godzinę
        "category": 0,
        "geo": "",
        "gprop": "",
        "schedule_interval": "@hourly"
    }
]

PYTRENDS_TIMEOUT = (15, 30)  # Zwiększenie timeoutu
