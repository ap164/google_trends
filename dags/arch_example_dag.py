from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta

# Funkcja Python, która będzie wykonywana w zadaniu
def my_task_function(**kwargs):
    print("Hello, Airflow!")
    return "Task completed successfully"

# Domyślne argumenty DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definicja DAG
with DAG(
    'example_dag',  # Nazwa DAG
    default_args=default_args,
    description='Przykładowy DAG w Airflow',
    schedule_interval=timedelta(days=1),  # Harmonogram (codziennie)
    start_date=datetime(2023, 4, 1),  # Data początkowa
    catchup=False,  # Wyłącz catchup
    tags=['example'],  # Tag dla organizacji
) as dag:

    # Definicja zadania
    task_1 = PythonOperator(
        task_id='print_hello',  # Unikalny identyfikator zadania
        python_callable=my_task_function,  # Funkcja do wykonania
    )

    # Możesz dodać więcej zadań i ustawić ich zależności
    # task_2 = PythonOperator(...)
    # task_1 >> task_2  # task_2 zależy od task_1