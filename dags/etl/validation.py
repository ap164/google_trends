import pandas as pd
import logging

logger = logging.getLogger(__name__)


def normalize_str_value(value: str) -> str:
    return value.strip().lower()

def normalize_country_names(country: str) -> str:
    return country.strip().title() 

def normalize_schedule_interval(schedule_interval: str) -> str:
    if not isinstance(schedule_interval, str):
        return schedule_interval

    if schedule_interval.startswith("@"):
        return schedule_interval[1:]

    parts = schedule_interval.strip().split()
    if len(parts) < 5:
        return schedule_interval

    minute, hour, day, month, day_of_week = parts[:5]

    if all(p == "*" for p in [hour, day, month, day_of_week]) and minute.startswith("*/"):
        return "minutely"
    elif minute == "0" and all(p == "*" for p in [hour, day, month, day_of_week]):
        return "hourly"
    elif minute == "0" and hour == "0" and all(p == "*" for p in [day, month, day_of_week]):
        return "daily"
    elif day_of_week != "*" and all(p == "*" for p in [day, month]):
        return "weekly"
    elif day != "*" and month == "*" and day_of_week == "*":
        return "monthly"
    elif month != "*":
        return "yearly"

    return schedule_interval

def resample_to_hourly(df: pd.DataFrame, value_col: str, keyword: str) -> pd.DataFrame:
    if isinstance(df, pd.DataFrame) and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        if len(df) > 1:
            min_diff = (df["date"].sort_values().diff().dropna().min())
            if pd.notnull(min_diff) and min_diff < pd.Timedelta(hours=1):
                df = (
                    df.set_index("date")
                    .resample("h")[value_col]
                    .mean()
                    .reset_index()
                )

        df[value_col] = df[value_col].apply(
            lambda x: round(float(x), 2) if isinstance(x, (int, float)) else x)
        logger.info(f"Data resampled to hourly for '{keyword}' and values rounded.")
    return df

def process_dataframe(data: pd.DataFrame) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        data = data.reset_index()

        if 'date' in data.columns:
            data['date'] = pd.to_datetime(data['date'], errors='coerce').dt.floor('min')

        if 'geoName' in data.columns:
            data['geoName'] = data['geoName'].apply(normalize_country_names)

        data = data.astype(object).where(pd.notnull(data), None)

        if data.isnull().sum().sum() > 0:
            logger.warning("Data contains missing values!")

    return data


def validate_base_input(data: dict, resample: bool = False) -> dict:
    required_fields = ['data', 'keyword', 'schedule_interval']
    for field in required_fields:
        if field not in data:
            raise ValueError(f"Missing required field: {field}")

    old_col = data['keyword']
    new_col = normalize_str_value(data['keyword'])
    if isinstance(data['data'], pd.DataFrame) and old_col in data['data'].columns:
        data['data'] = data['data'].rename(columns={old_col: new_col})

    if isinstance(data['data'], pd.DataFrame):
        df = process_dataframe(data['data'])
        if resample and "date" in df.columns:
            value_cols = [col for col in df.columns if col not in ("date", "isPartial", "index")]
            if value_cols:
                value_col = value_cols[0]
                df = resample_to_hourly(df, value_col, new_col)
        data['data'] = df
    return data

def validate_interest_over_time_input(data: dict) -> dict:
    validated = validate_base_input(data, resample=True)
    if 'channel' not in data:
        raise ValueError("Missing required field: channel")
    validated['channel'] = normalize_str_value(data['channel'])
    return validated

def validate_interest_by_region_input(data: dict) -> dict:
    validated = validate_base_input(data, resample=False)
    if 'data' in validated and isinstance(validated['data'], pd.DataFrame):
        df = validated['data']
        # find the value column
        value_cols = [col for col in df.columns if col not in ("geoName", "index")]
        if value_cols:
            value_col = value_cols[0]
            df = convert_region_values_to_int(df, value_col)
        validated['data'] = process_dataframe(df)
    return validated

def convert_region_values_to_int(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    if value_col in df.columns:
        df[value_col] = df[value_col].apply(
            lambda x: int(round(x)) if pd.notnull(x) and isinstance(x, float) else (int(x) if isinstance(x, int) else None)
        )
    return df

