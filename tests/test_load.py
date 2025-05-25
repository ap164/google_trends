import pandas as pd
from unittest.mock import MagicMock, patch
import datetime
import sys
import os


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

from etl.load import (load_interest_over_time, load_interest_by_region)


def test_load_interest_over_time_success():
    data = pd.DataFrame({
        "date": [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
        "keyword": [10, 20],
        "isPartial": [False, False]
    })
    cursor = MagicMock()
    connection = MagicMock()
    cursor.rowcount = 2

    load_interest_over_time(cursor, connection, data, "test", "web", "daily")

    assert cursor.executemany.called
    assert connection.commit.called
    cursor.executemany.assert_called_once()
    connection.commit.assert_called_once()

def test_load_interest_over_time_duplicates_logged():
    data = pd.DataFrame({
        "date": [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
        "keyword": [10, 20],
        "isPartial": [False, False]
    })
    cursor = MagicMock()
    connection = MagicMock()
    cursor.rowcount = 1  

    with patch("etl.load.logger") as mock_logger:
        load_interest_over_time(cursor, connection, data, "test", "web", "daily")
        assert mock_logger.info.call_count == 2
        assert any(f"Skipped {cursor.rowcount} records due to duplicates" in str(call) for call in mock_logger.info.call_args_list)

def test_load_interest_over_time_missing_column_logs_error():
    # Brakuje kolumny "date"
    data = pd.DataFrame({
        "keyword": [10],
        "isPartial": [False]
    })
    cursor = MagicMock()
    connection = MagicMock()

    with patch("etl.load.logger") as mock_logger:
        load_interest_over_time(cursor, connection, data, "test", "web", "daily")
        assert mock_logger.error.called and "Error while loading interest_over_time" in mock_logger.error.call_args[0][0]
        


def test_load_interest_by_region_success():
    data = pd.DataFrame({
        "geoName": ["PL", "DE"],
        "value": [1, 2]
    })
    cursor = MagicMock()
    connection = MagicMock()
    cursor.rowcount = 2

    load_interest_by_region(cursor, connection, data, "test", "daily")

    assert cursor.executemany.called
    assert connection.commit.called

def test_load_interest_by_region_duplicates_logged():
    data = pd.DataFrame({
        "geoName": ["PL", "DE"],
        "value": [1, 2]
    })
    cursor = MagicMock()
    connection = MagicMock()
    cursor.rowcount = 1  # 1 inserted, 1 duplicate

    with patch("etl.load.logger") as mock_logger:
        load_interest_by_region(cursor, connection, data, "test", "daily")
        assert mock_logger.info.call_count == 2
        assert any(f"Skipped {cursor.rowcount} records due to duplicates" in str(call) for call in mock_logger.info.call_args_list)

def test_load_interest_by_region_exception_logged():
    data = pd.DataFrame({
        "geoName": None,
        "wrong column name": [1, 2]
    })
    cursor = MagicMock()
    connection = MagicMock()


    with patch("etl.load.logger") as mock_logger:
        load_interest_by_region(cursor, connection, data, "test", "daily")
        assert mock_logger.error.called and "Error while loading interest_by_region" in mock_logger.error.call_args[0][0]
