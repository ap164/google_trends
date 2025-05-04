from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
from pytrends_helpers import fetch_and_store_pytrends
from config import PYTRENDS_CONFIGS, LOG_LEVEL
import logging
from collections import defaultdict

# Konfiguracja logowania
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Grupowanie konfiguracji na podstawie harmonogramu
grouped_configs = defaultdict(list)
for config in PYTRENDS_CONFIGS:
    grouped_configs[config["schedule_interval"]].append(config)

# Tworzenie DAG-ów na podstawie grup
for schedule_interval, configs in grouped_configs.items():
    dag_id = f"pytrends_{schedule_interval.strip('@')}"  # Unikalny ID DAG-a

    # Konkatenacja wszystkich słów kluczowych dla tagów
    all_keywords = [keyword for config in configs for keyword in config["keywords"]]

    # all_keywords = []  # Pusta lista na wynik
    # for config in configs:  # Iteracja po każdym słowniku w configs
    #     for keyword in config["keywords"]:  # Iteracja po słowach kluczowych w config["keywords"]
    #         all_keywords.append(keyword)  # Dodanie słowa kluczowego do listy

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Pobieranie danych z PyTrends ({schedule_interval})",
        schedule_interval=schedule_interval,
        start_date=datetime(2024, 9, 25),
        catchup=False,
        tags=all_keywords 
    ) as dag:
        run_pytrends = PythonOperator(
            task_id=f"fetch_and_store_pytrends_{schedule_interval.strip('@')}",
            python_callable=fetch_and_store_pytrends,
            op_kwargs={"configs": configs},  # Przekazanie grupy konfiguracji do funkcji
        )

        globals()[dag_id] = dag  # Rejestracja DAG-a w globalnym zakresie
