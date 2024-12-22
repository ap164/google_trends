from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='123',
    default_args=default_args,
    description="dag_with_postgres",
    start_date=datetime(2024, 7, 18),
    schedule_interval='@daily',
    catchup=False  # To prevent backfilling of past dates
) as dag:
    # task1 = PostgresOperator(
    #     task_id='create_postgres_table',
    #     postgres_conn_id='postgres_default',
    #     sql="""
    #         CREATE TABLE IF NOT EXISTS tab1 (
    #             dt DATE,
    #             dag_id CHARACTER VARYING,
    #             PRIMARY KEY (dt, dag_id)
    #         )
    #     """
    # )

    task2 = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO tab1 (dt, dag_id) VALUES ('{{ ds }}', '{{ dag.dag_id }}')
        """
    )

    task3 = PostgresOperator(
        task_id='delete_from_table',
        postgres_conn_id='postgres_default',
        sql="""
            DELETE FROM tab1 
            WHERE dt = '{{ ds }}' 
            AND dag_id = '{{ dag.dag_id }}'
        """
    )

    task3 >> task2
