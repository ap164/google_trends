from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytrends_helpers import fetch_and_store_pytrends


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 25),  
    'email_on_failure': True,
    'email': ['prusa5100@gmail.com'],  
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='pytrends_dag',
    default_args=default_args,
    description='Pobieranie danych z Pytrends i zapis do PostgreSQL',  
    schedule_interval= None, #'0 08 * * *',  
    catchup=False  
) as dag:  
    run_pytrends = PythonOperator(
        task_id='fetch_and_store_pytrends',
        python_callable=fetch_and_store_pytrends,
    )
