import pandas as pd
import pytest
from unittest.mock import patch

from etl.validation import (
    normalize_str_value,
    normalize_country_names,
    normalize_schedule_interval,
    resample_to_hourly,
    process_dataframe,
    validate_base_input,
    validate_interest_over_time_input,
    validate_interest_by_region_input,
    convert_region_values_to_int
)

def test_normalize_str_value():
    assert normalize_str_value(" kEywORd ") == "keyword"

def test_normalize_country_names():
    assert normalize_country_names("united staTes ") == "United States"

@pytest.mark.parametrize("cron,expected", [
    ("@hourly", "hourly"),
    ("@daily", "daily"),
    ("  0 0 * * *  ", "daily"),
    ("0 0 1 1 *", "yearly"),
    ("15 14 1 * *", "monthly"),
    ("0 0 1 * *", "monthly"),
    ("0 0 * * 1", "weekly"),
    ("0 0 * * 0", "weekly"),
    ("0 0 * * *", "daily"),
    ("0 * * * *", "hourly"),
    ("*/5 * * * *", "minutely"),
    ("*/1 * * * *", "minutely"),
    ("0 * * *", "0 * * *"),
    ("0 0 * *", "0 0 * *"),
    ("0 0 *", "0 0 *"),
])
def test_normalize_schedule_interval(cron, expected):
    assert normalize_schedule_interval(cron) == expected

def test_resample_to_hourly():
    df1 = pd.DataFrame({
        "date": pd.to_datetime(["2023-05-18 12:10", "2023-05-18 12:20", "2023-05-18 12:50"]),
        "value": [1, 3, 5]
    })
    result1 = resample_to_hourly(df1, "value", "test1")
    assert (result1["date"] == pd.to_datetime("2023-05-18 12:00")).all()
    assert abs(result1["value"].iloc[0] - 3.0) < 1e-6
    assert len(result1) == 1

    df2 = pd.DataFrame({
        "date": pd.to_datetime(["2023-05-18 12:00", "2023-05-18 13:00", "2023-05-18 14:00"]),
        "value": [2, 4, 6]
    })
    result2 = resample_to_hourly(df2, "value", "test2")
    pd.testing.assert_frame_equal(result2, df2)

    df3 = pd.DataFrame({
        "time": pd.to_datetime(["2023-05-18 12:00", "2023-05-18 12:30"]),
        "value": [7, 9]
    })
    pd.testing.assert_frame_equal(resample_to_hourly(df3, "value", "test3"), df3)

    df4 = pd.DataFrame({
        "date": pd.to_datetime(["2023-05-18 10:00", "2023-05-18 10:30"]),
        "value": [1.12345, 2.98765]
    })
    result4 = resample_to_hourly(df4, "value", "test4")
    assert all(result4["value"].apply(lambda x: round(x, 2) == x))
    assert (result4["date"] == pd.to_datetime("2023-05-18 10:00")).all()


def test_process_dataframe(monkeypatch):
    def mock_normalize_country_names(value):
        return "MockedCountry"
    
    monkeypatch.setattr("etl.validation.normalize_country_names", mock_normalize_country_names)
    
    df1 = pd.DataFrame({
        "date": ["2023-05-18 12:34:56", "2023-05-18 13:45:10"],
        "geoName": ["poland", " united states "],
        "value": [1, 2]
    }, index=[10, 20])
    
    result1 = process_dataframe(df1)
    
    assert result1.index.equals(pd.RangeIndex(0, 2))
    assert all(result1["date"] == pd.to_datetime(["2023-05-18 12:34", "2023-05-18 13:45"]))
    # Sprawdź, że wszystkie geoName są zamockowane
    assert list(result1["geoName"]) == ["MockedCountry", "MockedCountry"]
    assert list(result1["value"]) == [1, 2]
    assert not result1.isnull().values.any()


    df2 = pd.DataFrame({
        "date": ["2023-05-18 10:00", None, "invalid date"],
        "value": [10, None, 30]
    })
    result2 = process_dataframe(df2)
    assert pd.isnull(result2.loc[1, "date"])
    assert pd.isnull(result2.loc[2, "date"])
    assert result2.loc[1, "value"] is None

    not_df = "not a dataframe"
    assert process_dataframe(not_df) == not_df

    df4 = pd.DataFrame({"a": [1, 2], "b": [3, 4]}, index=[5, 6])
    result4 = process_dataframe(df4)
    assert result4.index.equals(pd.RangeIndex(0, 2))
    assert result4.loc[0, "a"] == 1
    assert result4.loc[1, "b"] == 4


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "old_keyword": [1, 2, 3],
        "date": pd.date_range("2024-01-01", periods=3, freq="min")
    })


def test_missing_required_fields():
    base_input = {
        'data': "dummy",
        'keyword': 'key',
        'schedule_interval': 'daily'
    }
    for missing in ['data', 'keyword', 'schedule_interval']:
        invalid_input = base_input.copy()
        invalid_input.pop(missing)
        with pytest.raises(ValueError, match=f"Missing required field: {missing}"):
            validate_base_input(invalid_input)


@patch("etl.validation.normalize_str_value")
@patch("etl.validation.process_dataframe")
def test_validate_base_input_column_renaming(mock_process, mock_normalize, sample_df):
    mock_normalize.return_value = "normalized_key"
    mock_process.return_value = sample_df.copy()

    data = {
        "data": sample_df.copy(),
        "keyword": "old_keyword",
        "schedule_interval": "daily"
    }

    df_renamed = sample_df.rename(columns={"old_keyword": "normalized_key"})
    mock_process.return_value = df_renamed

    result = validate_base_input(data)
    assert "normalized_key" in result['data'].columns
    assert "old_keyword" not in result['data'].columns
    mock_normalize.assert_called_once_with("old_keyword")


@patch("etl.validation.normalize_str_value")
@patch("etl.validation.process_dataframe")
@patch("etl.validation.resample_to_hourly")
def test_validate_base_input_resample_applied_if_flag_true(mock_resample, mock_process, mock_normalize, sample_df):
    mock_normalize.return_value = "normalized_key"
    mock_process.return_value = sample_df.copy()
    mock_resample.return_value = sample_df.copy()

    data = {
        "data": sample_df.copy(),
        "keyword": "old_keyword",
        "schedule_interval": "daily"
    }

    result = validate_base_input(data, resample=True)
    assert isinstance(result['data'], pd.DataFrame)


def test_validate_interest_over_time_input_success():
    df = pd.DataFrame({
        "keyword": [1, 2],
        "date": pd.date_range("2024-01-01", periods=2, freq="h")
    })
    data = {
        "data": df,
        "keyword": "keyword",
        "schedule_interval": "daily",
        "channel": "WEB"
    }
    result = validate_interest_over_time_input(data)
    assert "channel" in result
    assert isinstance(result["data"], pd.DataFrame)


def test_validate_interest_over_time_input_missing_channel():
    df = pd.DataFrame({
        "keyword": [1, 2],
        "date": pd.date_range("2024-01-01", periods=2, freq="h")
    })
    data = {
        "data": df,
        "keyword": "keyword",
        "schedule_interval": "daily"
    }
    with pytest.raises(ValueError, match="Missing required field: channel"):
        validate_interest_over_time_input(data)


def test_convert_region_values_to_int_float_and_int():
    df = pd.DataFrame({
        "geoName": ["PL", "DE", "FR"],
        "value": [1.0, 2, 3.5]
    })
    result = convert_region_values_to_int(df.copy(), "value")
    assert result["value"].tolist() == [1, 2, 4]


def test_convert_region_values_to_int_missing_column():
    df = pd.DataFrame({
        "geoName": ["PL", "DE"]
    })
    result = convert_region_values_to_int(df.copy(), "value")
    assert "value" not in result.columns

def test_validate_interest_by_region_input_success():
    df = pd.DataFrame({
        "geoName": ["Poland", "Germany"],
        "value": [1.0, 2.0]
    })
    data = {
        "data": df,
        "keyword": "value",
        "schedule_interval": "daily"
    }
    result = validate_interest_by_region_input(data)
    assert "data" in result
    assert isinstance(result["data"], pd.DataFrame)
    assert result["data"]["value"].tolist() == [1, 2]

def test_validate_interest_by_region_input_non_df():
    data = {
        "data": "not a dataframe",
        "keyword": "value",
        "schedule_interval": "daily"
    }
    result = validate_interest_by_region_input(data)
    assert result["data"] == "not a dataframe"
