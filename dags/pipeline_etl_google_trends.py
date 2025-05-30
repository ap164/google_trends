from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.extract import extract_interest_over_time, extract_interest_by_region
from etl.load import load_interest_over_time, load_interest_by_region
from etl.transform import transform_interest_over_time, transform_interest_by_region
from utils import connect_to_postgres, load_config, send_email
from pytrends.request import TrendReq
import logging
import time
import os



config = load_config()

LOG_LEVEL = config.get("log_level", "WARNING")
POSTGRES = config["postgres"]
EMAIL = config["email"]
PYTRENDS_CONFIGS = config.get("pytrends_configs", [])
PYTRENDS_TIMEOUT = tuple(config.get("pytrends_timeout", [20, 40]))

RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
SENDER_EMAIL_PASSWORD = os.environ.get("SENDER_EMAIL_PASSWORD")

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': [RECIPIENT_EMAIL],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def handle_etl(keyword, data_type, config, pytrends, cursor, connection):
    try:
        if data_type == "interest_over_time":
            params = {k: v for k, v in config["interest_over_time"].items() if k != "schedule_interval"}
            result = extract_interest_over_time(pytrends, keyword, **params)
            if result is None:
                logger.warning(f"No data for '{keyword}'")
            elif isinstance(result, str):
                if "429" in result:
                    logger.warning(f"Error 429 while fetching data for '{keyword}'")
                    return "429"
                elif "Google Trends returned an empty response" in result:
                    logger.warning(f"Google Trends returned an empty response for '{keyword}'")
                else:
                    logger.warning(f"Unknown error for '{keyword}': {result}")
                    return "skip"

            transformed_data, normalized_schedule_interval, normalized_keyword = transform_interest_over_time(
                result, keyword,
                config["interest_over_time"]["gprop"],
                config["interest_over_time"]["schedule_interval"])
            load_interest_over_time(cursor, connection, transformed_data, normalized_keyword, config["interest_over_time"]["gprop"], normalized_schedule_interval, config["interest_over_time"]["geo"])
        elif data_type == "interest_by_region":
            result = extract_interest_by_region(
                pytrends, keyword,
                config["interest_by_region"]["category"],
                config["interest_by_region"]["geo"],
                config["interest_by_region"]["gprop"]
            )
            if result is None:
                logger.warning(f"No data for '{keyword}'")
            elif isinstance(result, str):
                if "429" in result:
                    logger.warning(f"Error 429 while fetching data for '{keyword}'")
                    return "429"
                elif "Google Trends returned an empty response" in result:
                    logger.warning(f"Google Trends returned an empty response for '{keyword}'")
                else:
                    logger.warning(f"Unknown error for '{keyword}': {result}")
                    return "skip"
            transformed_data, normalized_schedule_interval, normalized_keyword = transform_interest_by_region(
                result, keyword,
                config["interest_by_region"]["schedule_interval"])
            load_interest_by_region(cursor, connection, transformed_data, normalized_keyword, normalized_schedule_interval,  config["interest_by_region"]["geo"])
        return "continue"
    except Exception as e:
        message = str(e)
        subject = f"ETL error for {data_type} ({keyword})"
        send_email(subject, message, EMAIL["recipient"], EMAIL["sender"], EMAIL["sender_password"])
        logger.error(f"Error in ETL process for {data_type} ({keyword}): {e}")
        raise

def run_etl(config, data_type):
    connection = connect_to_postgres(POSTGRES["host"], POSTGRES["db"], POSTGRES["user"], POSTGRES["password"])
    if not connection:
        raise ConnectionError("Failed to connect to the database.")
    cursor = connection.cursor()
    pytrends = TrendReq(hl='en-US', tz=360, timeout=PYTRENDS_TIMEOUT, retries=5, backoff_factor=5)

    failed_429 = []
    for keyword in config["keywords"]:
        result = handle_etl(keyword, data_type, config, pytrends, cursor, connection)
        if result == "429":
            failed_429.append(keyword)

    if failed_429:
        time.sleep(600)
        logger.info(f"Retry for keywords: {', '.join(failed_429)}")
        still_failed = []
        for keyword in failed_429:
            result = handle_etl(keyword, data_type, config, pytrends, cursor, connection)
            if result == "429":
                still_failed.append(keyword)
        if still_failed:
            subject = f"Error 429: Failed to fetch data after retry for {data_type}"
            message = f"Failed to fetch data for keywords even after retry:\n{', '.join(still_failed)}"
            send_email(subject, message, EMAIL["recipient"], EMAIL["sender"], EMAIL["sender_password"])
            logger.error(message)
    connection.close()

def create_dag(dag_id, schedule_interval, keywords, config, data_type):
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=datetime(2024, 9, 25),
        catchup=False,
        tags=keywords
    ) as dag:
        PythonOperator(
            task_id=f"run_{data_type}",
            python_callable=run_etl,
            op_kwargs={"config": config, "data_type": data_type}
        )
    return dag

for config in PYTRENDS_CONFIGS:
    topic = config["topic"]
    keywords = config["keywords"]
    for data_type in ["interest_over_time", "interest_by_region"]:
        section = config.get(data_type)
        if section and "schedule_interval" in section:
            dag_id = f"etl_{topic}_{data_type}"
            schedule_interval = section["schedule_interval"]
            dag = create_dag(dag_id, schedule_interval, keywords, config, data_type)
            globals()[dag_id] = dag
