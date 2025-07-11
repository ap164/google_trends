import logging
import datetime

logger = logging.getLogger(__name__)
   
def load_interest_over_time(cursor, connection, data, keyword, channel, schedule_interval, geo_search):
    """
    Loads Google Trends 'interest over time' data into the database.

    Args:
        cursor: Database cursor object.
        connection: Database connection object.
        data (DataFrame): DataFrame containing interest over time data.
        keyword (str): Keyword for which data was retrieved.
        channel (str): Channel type (e.g., 'web', 'news', etc.).
        schedule_interval (str): Schedule interval for the data load.
        geo_search (str): Country or region code (e.g., 'PL').
    """
    try:
        value_col = [col for col in data.columns if col not in ("index", "date", "isPartial")][0]
        rows = [
            (row["date"], keyword, row[value_col], channel, schedule_interval, None if geo_search == '' else geo_search)
            for _, row in data.iterrows()
        ]
        insert_query = """
        INSERT INTO AIRFLOW_DB.interest_over_time (date, keyword, value, channel, schedule_interval, geo_search)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        cursor.executemany(insert_query, rows)
        inserted = cursor.rowcount
        connection.commit()

        duplicates = len(rows) - inserted
        logger.info(f"Loaded interest_over_time for '{keyword}': {inserted} new records.")
        if duplicates > 0:
            logger.info(f"Skipped {duplicates} records due to duplicates (deduplication ON CONFLICT).")
    except Exception as e:
        logger.error(f"Error while loading interest_over_time for '{keyword}': {e}")


def load_interest_by_region(cursor, connection, data, keyword, schedule_interval, geo_search):
    """
    Loads Google Trends 'interest by region' data into the database.

    Args:
        cursor: Database cursor object.
        connection: Database connection object.
        data (DataFrame): DataFrame containing interest by region data.
        keyword (str): Keyword for which data was retrieved.
        schedule_interval (str): Schedule interval for the data load.
        geo_search (str): Country or region code (e.g., 'PL').
    """
    try:
        today_date = datetime.datetime.now().date()
        # Get the value column name from dataframe (excluding 'index' and 'geoName')
        value_col = [col for col in data.columns if col not in ("index", "geoName")][0]
        rows = [
            (row["geoName"], keyword, row[value_col], schedule_interval, today_date, None if geo_search == '' else geo_search)
            for _, row in data.iterrows()
        ]
        insert_query = """
        INSERT INTO AIRFLOW_DB.interest_by_region (region, keyword, value, schedule_interval, load_date, geo_search)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        cursor.executemany(insert_query, rows)
        inserted = cursor.rowcount
        connection.commit()

        duplicates = len(rows) - inserted
        logger.info(f"Loaded interest_by_region for '{keyword}': {inserted} new records.")
        if duplicates > 0:
            logger.info(f"Skipped {duplicates} records due to duplicates (deduplication ON CONFLICT).")
    except Exception as e:
        logger.error(f"Error while loading interest_by_region for '{keyword}': {e}")
