from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os

from utils import connect_to_postgres, load_config, send_email

config = load_config()
POSTGRES = config["postgres"]
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
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def execute_sql_query():
    """
    Executes a set of SQL statements to generate and update Google Trends BI reports in the PostgreSQL database.

    This function connects to the PostgreSQL database, runs a series of SQL commands to:
    - Refresh the interest_by_region_report_new table with the latest data.
    - Create and update the interest_over_time_report_history table with daily aggregated values.
    - Update the interest_over_time_report_avg_value_hour table with average values per hour.
    Commits all changes and closes the connection.

    Raises:
        ConnectionError: If the database connection fails.
        Exception: If SQL execution fails.
    """
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
        /*   geo    */
        DELETE FROM AIRFLOW_REPORTS.interest_by_region_report_new;
        INSERT INTO AIRFLOW_REPORTS.interest_by_region_report_new
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
                WHEN LOWER(a.keyword) IN ('gcp','azure','aws') THEN 'data warehousing tools'
                WHEN LOWER(a.keyword) IN ('python','java','sql','spark') THEN 'programming language' 
                WHEN LOWER(a.keyword) IN ('data scientist', 'data engineer', 'bi developer', 'data analyst', 'data architect') THEN 'job positions' END AS category,

            max(a.load_date) as load_date
        FROM airflow_db.interest_by_region a
        INNER join max_load_date b ON a.keyword = b.keyword AND a.load_date = b.max_load_date
        where geo_search  is null
        group by 1,2,3,4;

        /* history */
        create table interest_over_time_report_history_new_day as (
        select DATE_TRUNC('day', date) as date,
        keyword,
        CASE WHEN LOWER(a.keyword) IN ('power bi','tableau','superset','looker') THEN 'data visualization tools'
                WHEN LOWER(a.keyword) IN ('gcp','azure','aws') THEN 'data warehousing tools'
                WHEN LOWER(a.keyword) IN ('python','java','sql','spark') THEN 'programming language' 
                WHEN LOWER(a.keyword) IN ('data scientist', 'data engineer', 'bi developer', 'data analyst', 'data architect') THEN 'job positions' END AS category,

                ROUND(AVG(value)::numeric, 2) AS avg_value
        from airflow_db.interest_over_time a 
        where geo_search is null
        group by 1,2,3);

        INSERT INTO AIRFLOW_REPORTS.interest_over_time_report_history
        select n.* from interest_over_time_report_history_new_day n
        left join AIRFLOW_REPORTS.interest_over_time_report_history h 
            on h.keyword = n.keyword and h.date=n.date
        where h.date is null;
        drop table interest_over_time_report_history_new_day;


        /* avg per hour */
        DELETE FROM AIRFLOW_REPORTS.interest_over_time_report_avg_value_hour;
        INSERT INTO AIRFLOW_REPORTS.interest_over_time_report_avg_value_hour
        SELECT
            LPAD(EXTRACT(HOUR FROM date)::text, 2, '0') || ':00' AS hour_of_day,
            CASE WHEN LOWER(a.keyword) IN ('power bi','tableau','superset','looker') THEN 'data visualization tools'
                WHEN LOWER(a.keyword) IN ('gcp','azure','aws') THEN 'data warehousing tools'
                WHEN LOWER(a.keyword) IN ('python','java','sql','spark') THEN 'programming language' 
                WHEN LOWER(a.keyword) IN ('data scientist', 'data engineer', 'bi developer', 'data analyst', 'data architect') THEN 'job positions' END AS category,
                keyword,
            AVG(value) AS avg_value 
        FROM airflow_db.interest_over_time a 
        where schedule_interval='daily'
        and geo_search is null
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

def send_success_email():
    """
    Sends a notification email indicating that the Google Trends BI report DAG has succeeded.

    Uses the configured sender and recipient email addresses and credentials.
    """
    send_email(
        subject="DAG BI_GOOGLE_TRENDS_REPORT succeeded - Airflow",
        message="The Google Trends report has been generated successfully.",
        recipient_email=RECIPIENT_EMAIL,
        sender_email=SENDER_EMAIL,
        sender_email_password=SENDER_EMAIL_PASSWORD
    )

with DAG(
    dag_id='bi_report_google_trends',
    description='DAG to execute Google Trends SQL report tab',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['postgres', 'send_success_email']
) as dag:

    sql_query = PythonOperator(
        task_id='bi_google_trends_report_sql',
        python_callable=execute_sql_query
    )

    send_success_email = PythonOperator(
        task_id='send_success_email',
        python_callable=send_success_email,
    )

    sql_query >> send_success_email  