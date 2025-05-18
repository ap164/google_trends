import sys
import os

# Add the dags directory (where etl is) to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

import pandas as pd
from unittest.mock import patch

from etl.transform import (
    transform_interest_over_time,
    transform_interest_by_region
)

@patch("etl.transform.validate_interest_over_time_input")
@patch("etl.transform.normalize_str_value", return_value="normalized_keyword")
@patch("etl.transform.normalize_schedule_interval", return_value="normalized_schedule_interval")

def test_transform_interest_over_time(
    mock_normalize_schedule_interval,
    mock_normalize_str_value,
    mock_validate_interest_over_time_input
):
    dummy_dt = pd.DataFrame({
        "date": ["2023-01-01", "2023-01-02"],
        "value": [1, 2]
    })
    mock_validate_interest_over_time_input.return_value = {"data": dummy_dt}

    result = transform_interest_over_time(dummy_dt, "word", "area", "cron")
    
    assert isinstance(result, tuple)
    assert isinstance(result[0], pd.DataFrame)


@patch("etl.transform.validate_interest_by_region_input")
@patch("etl.transform.normalize_str_value", return_value="normalized_keyword")
@patch("etl.transform.normalize_schedule_interval", return_value="normalized_schedule_interval")

def test_transform_interest_by_region(
    mock_normalize_schedule_interval,
    mock_normalize_str_value,
    mock_validate_interest_by_region_input
):
    dummy_dt = pd.DataFrame({
        "region": ["US", "CA"],
        "value": [1, 2]
    })
    mock_validate_interest_by_region_input.return_value = {"data": dummy_dt}

    result = transform_interest_by_region(dummy_dt, "word", "cron")
    
    assert isinstance(result, tuple)
    assert isinstance(result[0], pd.DataFrame)
