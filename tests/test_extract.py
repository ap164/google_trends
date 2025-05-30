import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

from etl.extract import (
    extract_interest_over_time, 
    extract_interest_by_region
)
@pytest.fixture
def mock_pytrends():
    return MagicMock()

@patch("etl.extract.time.sleep", return_value=None)
def test_extract_interest_over_time_success(mock_sleep, mock_pytrends):
    mock_pytrends.interest_over_time.return_value = pd.DataFrame({"a": [1]})
    result = extract_interest_over_time(
        mock_pytrends, "test", "now 7-d", 0, "", ""
    )
    assert isinstance(result, pd.DataFrame)


@patch("etl.extract.time.sleep", return_value=None)
def test_extract_interest_over_time_empty_df(mock_sleep, mock_pytrends):
    mock_pytrends.build_payload.return_value = None
    mock_pytrends.interest_over_time.return_value = pd.DataFrame()
    result = extract_interest_over_time(
        mock_pytrends, "test", "now 7-d", 0, "", ""
    )
    assert result == "Google Trends returned an empty response"

@patch("etl.extract.time.sleep", return_value=None)
def test_extract_interest_over_time_none(mock_sleep, mock_pytrends):
    mock_pytrends.build_payload.return_value = None
    mock_pytrends.interest_over_time.return_value = None
    result = extract_interest_over_time(
        mock_pytrends, "test", "now 7-d", 0, "", ""
    )
    assert result == "Google Trends returned an empty response"

@patch("etl.extract.time.sleep", return_value=None)
def test_extract_interest_over_time_429_error(mock_sleep, mock_pytrends):
    mock_pytrends.build_payload.side_effect = Exception("429 Too Many Requests")
    result = extract_interest_over_time(
        mock_pytrends, "test", "now 7-d", 0, "", ""
    )
    assert result == "429: Too many requests"

@patch("etl.extract.time.sleep", return_value=None)
def test_extract_interest_over_time_other_error(mock_sleep, mock_pytrends):
    mock_pytrends.build_payload.side_effect = Exception("Some other error")
    result = extract_interest_over_time(
        mock_pytrends, "test", "now 7-d", 0, "", ""
    )
    assert result == "Some other error"




@patch("etl.extract.time.sleep", return_value=None)
def test_extract_interest_by_region_success(mock_sleep, mock_pytrends):
    mock_pytrends.interest_by_region.return_value = pd.DataFrame({"a": [1]})
    result = extract_interest_by_region(
        mock_pytrends, "test", 0, "", ""
    )
    assert isinstance(result, pd.DataFrame)


@patch("etl.extract.time.sleep", return_value=None)
def test_extract_interest_by_region_empty_df(mock_sleep, mock_pytrends):
    mock_pytrends.build_payload.return_value = None
    mock_pytrends.interest_by_region.return_value = pd.DataFrame()
    result = extract_interest_by_region(
        mock_pytrends, "test", 0, "", ""
    )
    assert result == "Google Trends returned an empty response"

@patch("etl.extract.time.sleep", return_value=None)
def test_extract_interest_by_region_none(mock_sleep, mock_pytrends):
    mock_pytrends.build_payload.return_value = None
    mock_pytrends.interest_by_region.return_value = None
    result = extract_interest_by_region(
        mock_pytrends, "test", 0, "", ""
    )
    assert result == "Google Trends returned an empty response"

@patch("etl.extract.time.sleep", return_value=None)
def test_extract_interest_by_region_429_error(mock_sleep, mock_pytrends):
    mock_pytrends.build_payload.side_effect = Exception("429 Too Many Requests")
    result = extract_interest_by_region(
        mock_pytrends, "test", 0, "", ""
    )
    assert result == "429: Too many requests"

@patch("etl.extract.time.sleep", return_value=None)
def test_extract_interest_by_region_other_error(mock_sleep, mock_pytrends):
    mock_pytrends.build_payload.side_effect = Exception("Some other error")
    result = extract_interest_by_region(
        mock_pytrends, "test", 0, "", ""
    )
    assert result == "Some other error"



@patch("etl.extract.time.sleep", return_value=None)
def test_extract_interest_by_region_country_level(mock_sleep):
    mock_pytrends = MagicMock()
    mock_pytrends.interest_by_region.return_value = pd.DataFrame({"a": [1]})
    
    result = extract_interest_by_region(
        mock_pytrends, keyword="Python", category=0, geo="", gprop=""
    )
    
    # Ensure the correct resolution was passed
    mock_pytrends.interest_by_region.assert_called_with(resolution="COUNTRY")
    assert isinstance(result, pd.DataFrame)


@patch("etl.extract.time.sleep", return_value=None)
def test_extract_interest_by_region_region_level(mock_sleep):
    mock_pytrends = MagicMock()
    mock_pytrends.interest_by_region.return_value = pd.DataFrame({"a": [1]})
    
    result = extract_interest_by_region(
        mock_pytrends, keyword="Python", category=0, geo="PL", gprop=""
    )
    
    # Ensure the correct resolution was passed
    mock_pytrends.interest_by_region.assert_called_with(resolution="REGION")
    assert isinstance(result, pd.DataFrame)
