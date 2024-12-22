from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def send_email_to_kafka():
    subprocess.run(["python", "/usr/src/app/kafka_scripts/email_csv_producer_streaming.py"])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['twoj_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'email_to_kafka',
    default_args=default_args,
    description='DAG do wysyłania nowych e-maili zawierających "raport" do Kafki',
    schedule_interval=timedelta(minutes=5),  # Uruchamiaj co 5 minut
    start_date=datetime(2024, 4, 27),
    catchup=False,
) as dag:

    send_email_task = PythonOperator(
        task_id='send_email_to_kafka',
        python_callable=send_email_to_kafka,
    )

    send_email_task
