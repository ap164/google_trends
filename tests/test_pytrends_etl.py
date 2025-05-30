import pytest
from unittest.mock import MagicMock, patch
import sys
import os

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

from pytrends_etl import (
    handle_etl, 
    run_etl
)
@pytest.fixture
def mock_config():
    return {
        "interest_over_time": {
            "geo": "PL",
            "gprop": "",
            "schedule_interval": "@daily"
        },
        "interest_by_region": {
            "category": 0,
            "geo": "PL",
            "gprop": "",
            "schedule_interval": "@daily"
        },
        "keywords": ["python"],
        "topic": "test",
        "log_level": "INFO"
    }

@pytest.fixture
def mock_pytrends():
    return MagicMock()

@pytest.fixture
def mock_cursor():
    return MagicMock()

@pytest.fixture
def mock_connection(mock_cursor):
    conn = MagicMock()
    conn.cursor.return_value = mock_cursor
    return conn


def test_handle_etl_number(
    mock_pytrends, mock_cursor, mock_connection
):
    config = {
        "topic": "data_visualization",
        "keywords": ["power bi", "tableau", "superset", "looker"],
        "interest_by_region": {
            "category": 0,
            "geo": "",
            "gprop": "",
            "schedule_interval": "01 15 1 * *"
        },
        "interest_over_time": {
            "timeframe": "now 1-d",
            "category": 0,
            "geo": "",
            "gprop": "",
            "schedule_interval": "01 15 2 * *"
        }
    }
    with patch("pytrends_etl.extract_interest_over_time") as mock_extract_time, \
         patch("pytrends_etl.transform_interest_over_time") as mock_transform_time, \
         patch("pytrends_etl.load_interest_over_time") as mock_load_time, \
         patch("pytrends_etl.extract_interest_by_region") as mock_extract_region, \
         patch("pytrends_etl.transform_interest_by_region") as mock_transform_region, \
         patch("pytrends_etl.load_interest_by_region") as mock_load_region, \
         patch("pytrends_etl.send_email"):
        
        mock_extract_time.return_value = mock_extract_region.return_value = pd.DataFrame({"date": ["2024-01-01"], "value": [100]})
        mock_transform_time.return_value = mock_transform_region.return_value = ("transformed", "interval", "keyword")
        
        for keyword in config["keywords"]:
            handle_etl(keyword, "interest_over_time", config, mock_pytrends, mock_cursor, mock_connection)
            handle_etl(keyword, "interest_by_region", config, mock_pytrends, mock_cursor, mock_connection)
        
        assert mock_extract_time.call_count + mock_extract_region.call_count == 8
        assert mock_transform_time.call_count + mock_transform_region.call_count == 8
        assert mock_load_time.call_count + mock_load_region.call_count == 8

@patch("pytrends_etl.send_email")
@patch("pytrends_etl.extract_interest_over_time")
@patch("pytrends_etl.transform_interest_over_time")
@patch("pytrends_etl.load_interest_over_time")
def test_handle_etl_transform_error(
    mock_load, mock_transform, mock_extract, mock_send_email,
    mock_config, mock_pytrends, mock_cursor, mock_connection
):
    # Simulate transform raising an error
    mock_extract.return_value = pd.DataFrame({"date": ["2024-01-01"], "value": [100]})
    mock_transform.side_effect = Exception("Transform failed")
    with pytest.raises(Exception):
        handle_etl("python", "interest_over_time", mock_config, mock_pytrends, mock_cursor, mock_connection)
    mock_send_email.assert_called_once()

@patch("pytrends_etl.connect_to_postgres")
@patch("pytrends_etl.handle_etl")
def test_run_etl_with_retry(
    mock_handle_etl, mock_connect, mock_config, mock_connection, mock_cursor
):
    mock_connect.return_value = mock_connection
    mock_handle_etl.side_effect = ["429", "ok"]  # First fail, second retry ok
    run_etl(mock_config, "interest_over_time")
    assert mock_handle_etl.call_count == 2

@patch("pytrends_etl.connect_to_postgres")
@patch("pytrends_etl.handle_etl")
@patch("pytrends_etl.send_email")
def test_run_etl_all_retries_fail(
    mock_send_email, mock_handle_etl, mock_connect, mock_config, mock_connection
):
    mock_connect.return_value = mock_connection
    mock_handle_etl.side_effect = ["429", "429"]  # Fail twice

    run_etl(mock_config, "interest_over_time")
    mock_send_email.assert_called_once()
