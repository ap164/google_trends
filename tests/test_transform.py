import sys
import os

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

from etl.transform import (
    transform_interest_over_time,
    transform_interest_by_region
)

def test_transform_interest_over_time(mocker):
    dummy_df = pd.DataFrame({
        "date": ["2023-01-01", "2023-01-02"],
        "value": [1, 2]
    })

    mocker.patch("etl.transform.normalize_str_value", return_value="normalized_keyword")
    mocker.patch("etl.transform.normalize_schedule_interval", return_value="normalized_schedule_interval")
    mock_validate = mocker.patch("etl.transform.validate_interest_over_time_input")
    mock_validate.return_value = {"data": dummy_df}

    result = transform_interest_over_time(dummy_df, "word", "area", "cron")

    assert isinstance(result, tuple)
    assert isinstance(result[0], pd.DataFrame)


def test_transform_interest_by_region(mocker):
    dummy_df = pd.DataFrame({
        "region": ["US", "CA"],
        "value": [1, 2]
    })

    mocker.patch("etl.transform.normalize_str_value", return_value="normalized_keyword")
    mocker.patch("etl.transform.normalize_schedule_interval", return_value="normalized_schedule_interval")
    mock_validate = mocker.patch("etl.transform.validate_interest_by_region_input")
    mock_validate.return_value = {"data": dummy_df}

    result = transform_interest_by_region(dummy_df, "word", "cron")

    assert isinstance(result, tuple)
    assert isinstance(result[0], pd.DataFrame)


