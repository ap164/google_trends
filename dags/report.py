from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from utils import connect_to_postgres, load_config

# Załaduj konfigurację
config = load_config()
POSTGRES = config["postgres"]

# Logger
logger = logging.getLogger(__name__)

# Domyślne parametry DAG-a
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Funkcja wykonująca zapytanie SQL
def execute_sql_query():
    connection = connect_to_postgres(
        host=POSTGRES["host"],
        db=POSTGRES["db"],
        user=POSTGRES["user"],
        password=POSTGRES["password"]
    )

    if not connection:
        raise ConnectionError("Could not connect to PostgreSQL database")

    try:
        cursor = connection.cursor()
        sql = """
-- tech
CREATE SCHEMA IF NOT EXISTS AIRFLOW_RAPORTS;

CREATE TABLE IF NOT exists airflow_raports.interest_by_region_report_new (
	region varchar(255) NULL,
	keyword varchar(255) NULL,
	value float8 NULL,
	category text NULL,
	load_date TIMESTAMP NULL
);
CREATE table IF NOT EXISTS airflow_raports.interest_over_time_report_history (
    date DATE NOT NULL,
    keyword VARCHAR(100) NOT NULL,
    category text NULL,
    avg_value DOUBLE PRECISION NOT NULL
);

CREATE table IF NOT EXISTS airflow_raports.interest_over_time_report_avg_value_hour (
	hour_of_day text NULL,
	category text NULL,
	keyword varchar(255) NULL,
	avg_value float8 NULL
);


/*   geo    */

DELETE FROM AIRFLOW_RAPORTS.interest_by_region_report_new;
INSERT INTO AIRFLOW_RAPORTS.interest_by_region_report_new
with max_load_date as (
	SELECT keyword,
        MAX(load_date) AS max_load_date
    FROM airflow_db.interest_by_region
    GROUP BY 1)
SELECT  
    a.region,
    a.keyword,
    a.value,
    CASE WHEN LOWER(a.keyword) IN ('power bi','tableau','superset','looker') THEN 'data visualization tools'
         WHEN LOWER(a.keyword) IN ('gcp','azure','aws','databricks','snowflake') THEN 'data warehousing tools'
         WHEN LOWER(a.keyword) IN ('python','java','sql','spark') THEN 'programming language' END AS category,
    max(a.load_date) as load_date
FROM airflow_db.interest_by_region a
INNER join max_load_date b ON a.keyword = b.keyword AND a.load_date = b.max_load_date
group by 1,2,3,4;



/* history */
create table interest_over_time_report_history_new_day as (
select DATE_TRUNC('day', date) as date,
   keyword,
   CASE WHEN LOWER(a.keyword) IN ('power bi','tableau','superset','looker') THEN 'data visualization tools'
        WHEN LOWER(a.keyword) IN ('gcp','azure','aws','databricks','snowflake') THEN 'data warehousing tools'
        WHEN LOWER(a.keyword) IN ('python','java','sql','spark') THEN 'programming language' END AS category,
         ROUND(AVG(value)::numeric, 2) AS avg_value
from airflow_db.interest_over_time a 
group by 1,2,3);

INSERT INTO AIRFLOW_RAPORTS.interest_over_time_report_history
select n.* from interest_over_time_report_history_new_day n
left join AIRFLOW_RAPORTS.interest_over_time_report_history h 
	on h.keyword = n.keyword and h.date=n.date
where h.date is null;
drop table interest_over_time_report_history_new_day;


/* avg per hour */

DELETE FROM AIRFLOW_RAPORTS.interest_over_time_report_avg_value_hour;
INSERT INTO AIRFLOW_RAPORTS.interest_over_time_report_avg_value_hour
SELECT
    LPAD(EXTRACT(HOUR FROM date)::text, 2, '0') || ':00' AS hour_of_day,
	CASE WHEN LOWER(a.keyword) IN ('power bi','tableau','superset','looker') THEN 'data visualization tools'
        WHEN LOWER(a.keyword) IN ('gcp','azure','aws','databricks','snowflake') THEN 'data warehousing tools'
        WHEN LOWER(a.keyword) IN ('python','java','sql','spark') THEN 'programming language' END AS category,
    keyword,
	AVG(value) AS avg_value 
FROM airflow_db.interest_over_time a 
GROUP BY 1,2,3;
        """
        cursor.execute(sql)
        connection.commit()
        logger.info("SQL query executed successfully.")
        cursor.close()
        connection.close()
    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        raise

# Definicja DAG-a
with DAG(
    dag_id='custom_sql_dag',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 9, 1),
    catchup=False,
    tags=['postgres', 'custom'],
) as dag:

    run_sql = PythonOperator(
        task_id='execute_custom_sql',
        python_callable=execute_sql_query
    )

    run_sql
