import sys
import os

# Add the dags directory (where etl is) to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

import pandas as pd
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
    

def test_normalize_schedule_interval():
    assert normalize_schedule_interval("@hourly") == "hourly"
    assert normalize_schedule_interval("@daily") == "daily"
    assert normalize_schedule_interval("  0 0 * * *  ") == "daily"
    assert normalize_schedule_interval("0 0 1 1 *") == "yearly"
    assert normalize_schedule_interval("15 14 1 * *") == "monthly"   
    assert normalize_schedule_interval("0 0 1 * *") == "monthly"
    assert normalize_schedule_interval("0 0 * * 1") == "weekly"
    assert normalize_schedule_interval("0 0 * * 0") == "weekly"
    assert normalize_schedule_interval("0 0 * * *") == "daily"
    assert normalize_schedule_interval("0 * * * *") == "hourly"
    assert normalize_schedule_interval("*/5 * * * *") == "minutely"
    assert normalize_schedule_interval("*/1 * * * *") == "minutely"
    assert normalize_schedule_interval("0 * * *") == "0 * * *"  
    assert normalize_schedule_interval("0 0 * *") == "0 0 * *"
    assert normalize_schedule_interval("0 0 *") == "0 0 *"




def test_resample_to_hourly():
    # Case 1: Data with intervals less than one hour (resampling expected)
    df1 = pd.DataFrame({
        "date": pd.to_datetime([
            "2023-05-18 12:10", 
            "2023-05-18 12:20", 
            "2023-05-18 12:50"
        ]),
        "value": [1, 3, 5]
    })
    result1 = resample_to_hourly(df1, "value", "test1")
    # Should resample to full hour and average the values (at 12:00)
    assert (result1["date"] == pd.to_datetime("2023-05-18 12:00")).all()
    assert abs(result1["value"].iloc[0] - 3.0) < 1e-6  # mean of (1+3+5)/3 = 3.0

    # Case 2: Data with exactly 1-hour intervals (no resampling)
    df2 = pd.DataFrame({
        "date": pd.to_datetime([
            "2023-05-18 12:00",
            "2023-05-18 13:00",
            "2023-05-18 14:00"
        ]),
        "value": [2, 4, 6]
    })
    result2 = resample_to_hourly(df2, "value", "test2")
    # Should NOT resample; original data remains unchanged
    assert result2.shape == df2.shape
    assert (result2["date"] == df2["date"]).all()
    assert (result2["value"] == df2["value"]).all()

    # Case 3: Data without "date" column — should return unchanged
    df3 = pd.DataFrame({
        "time": pd.to_datetime([
            "2023-05-18 12:00",
            "2023-05-18 12:30"
        ]),
        "value": [7, 9]
    })
    result3 = resample_to_hourly(df3, "value", "test3")
    assert (result3 == df3).all().all()

    # Case 4: Float values with many decimal places (check rounding)
    df4 = pd.DataFrame({
        "date": pd.to_datetime([
            "2023-05-18 10:00",
            "2023-05-18 10:30"
        ]),
        "value": [1.12345, 2.98765]
    })
    result4 = resample_to_hourly(df4, "value", "test4")
    # Check that values are rounded to 2 decimal places
    assert all(result4["value"].apply(lambda x: round(x, 2) == x))
    # Check the date after resampling
    assert (result4["date"] == pd.to_datetime("2023-05-18 10:00")).all()



def test_process_dataframe():
    # Case 1: Normal DataFrame with date and geoName columns
    df1 = pd.DataFrame({
        "date": ["2023-05-18 12:34:56", "2023-05-18 13:45:10"],
        "geoName": ["poland", " united states "],
        "value": [1, 2]
    }, index=[10, 20])
    result1 = process_dataframe(df1)
    # Index should be reset to default (0,1)
    assert result1.index.equals(pd.RangeIndex(start=0, stop=2))
    # Dates should be datetime floored to minutes (seconds removed)
    assert all(result1['date'] == pd.to_datetime(["2023-05-18 12:34", "2023-05-18 13:45"]))
    # geoName should be normalized (title case, stripped)
    assert list(result1['geoName']) == ["Poland", "United States"]
    # Values should remain unchanged
    assert list(result1['value']) == [1, 2]
    # No null values expected here
    assert not result1.isnull().any().any()

    # Case 2: DataFrame with missing values and without geoName
    df2 = pd.DataFrame({
        "date": ["2023-05-18 10:00", None, "invalid date"],
        "value": [10, None, 30]
    })
    result2 = process_dataframe(df2)
    # date column: invalid date coerced to NaT, then floored -> NaT remains
    assert pd.isnull(result2.loc[1, 'date'])
    assert pd.isnull(result2.loc[2, 'date'])
    # Missing values replaced with None (object dtype)
    assert result2.loc[1, 'value'] is None
    # Warning about missing values expected (cannot assert logger here without mocking)

    # Case 3: Input is not a DataFrame (should return as is)
    not_df = "not a dataframe"
    result3 = process_dataframe(not_df)
    assert result3 == not_df

    # Case 4: DataFrame without 'date' and 'geoName' columns
    df4 = pd.DataFrame({
        "a": [1, 2],
        "b": [3, 4]
    }, index=[5,6])
    result4 = process_dataframe(df4)
    # Index reset
    assert result4.index.equals(pd.RangeIndex(start=0, stop=2))
    # Values unchanged
    assert result4.loc[0, "a"] == 1
    assert result4.loc[1, "b"] == 4



def test_validate_base_input():
    # Case 1: missing required field -> should raise ValueError
    try:
        validate_base_input({"keyword": "col", "schedule_interval": "hourly"})
    except ValueError as e:
        assert str(e) == "Missing required field: data"
    else:
        assert False, "Expected ValueError for missing 'data'"

    # Case 2: DataFrame with keyword column that should be renamed
    df = pd.DataFrame({
        "MyCol": [1, 2, 3],
        "date": pd.to_datetime(["2023-05-18 12:00", "2023-05-18 13:00", "2023-05-18 14:00"])
    })
    input_data = {
        "data": df,
        "keyword": "MyCol",
        "schedule_interval": "hourly"
    }
    output = validate_base_input(input_data, resample=False)
    # Column 'MyCol' renamed to normalized version (mycol)
    assert "mycol" in output['data'].columns
    assert "MyCol" not in output['data'].columns
    # DataFrame content preserved
    assert list(output['data']['mycol']) == [1, 2, 3]

    # Case 3: resample = True, data has date and value columns -> resampling applied
    df3 = pd.DataFrame({
        "keyword_col": [10, 20, 30],
        "date": pd.to_datetime(["2023-05-18 12:10", "2023-05-18 12:20", "2023-05-18 12:50"])
    })
    input_data3 = {
        "data": df3,
        "keyword": "keyword_col",
        "schedule_interval": "daily"
    }
    output3 = validate_base_input(input_data3, resample=True)
    # After resample, index should be rounded to full hour (12:00)
    assert all(output3['data']['date'] == pd.to_datetime("2023-05-18 12:00"))
    # Value should be average of original values
    assert abs(output3['data']['keyword_col'].iloc[0] - 20) < 1e-6

    # Case 4: input['data'] is not a DataFrame -> should skip processing
    input_data4 = {
        "data": "not a dataframe",
        "keyword": "anycol",
        "schedule_interval": "hourly"
    }
    output4 = validate_base_input(input_data4)
    # Should return original data unchanged
    assert output4['data'] == "not a dataframe"


