from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore

default_args = {
    'owner': 'admin',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='test',
    default_args=default_args,
    description="first dag",  
    start_date=datetime(2023, 12, 19),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello'  
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo "hey, I am banana"'  # Corrected command
    )
    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo "3 task lolo"'  # Corrected command
    )    
    task1 >> [task2, task3]
