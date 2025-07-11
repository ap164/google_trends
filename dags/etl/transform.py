import logging
import pandas as pd
from etl.validation import (
    validate_interest_over_time_input,
    validate_interest_by_region_input,
    normalize_str_value,
    normalize_schedule_interval
)

logger = logging.getLogger(__name__)

def transform_interest_over_time(data, keyword, channel, schedule_interval):
    """
    Transforms and validates Google Trends 'interest over time' data.

    Args:
        data (DataFrame): Raw interest over time data.
        keyword (str): Keyword for which data was retrieved.
        channel (str): Channel type (e.g., 'web', 'news', etc.).
        schedule_interval (str): Schedule interval for the data load.

    Returns:
        tuple: (Transformed DataFrame, normalized schedule_interval, normalized keyword)
    """
    try:
        logger.info(f"Transforming interest_over_time: '{keyword}'")

        normalized_keyword = normalize_str_value(keyword)
        normalized_channel = normalize_str_value(channel)
        normalized_schedule_interval = normalize_schedule_interval(schedule_interval)

        input_dict = {
            "data": data,
            "keyword": normalized_keyword,
            "channel": normalized_channel,
            "schedule_interval": normalized_schedule_interval
        }

        validated = validate_interest_over_time_input(input_dict)
        df = validated["data"]

        logger.info(f"Normalized data for '{normalized_keyword}'")

        return df, normalized_schedule_interval, normalized_keyword

    except Exception as e:
        logger.error(f"Error while transforming interest_over_time for '{keyword}': {e}")
        return None, schedule_interval, keyword


def transform_interest_by_region(data, keyword, schedule_interval):
    """
    Transforms and validates Google Trends 'interest by region' data.

    Args:
        data (DataFrame): Raw interest by region data.
        keyword (str): Keyword for which data was retrieved.
        schedule_interval (str): Schedule interval for the data load.

    Returns:
        tuple: (Transformed DataFrame, normalized schedule_interval, normalized keyword)
    """
    try:
        logger.info(f"Transforming interest_by_region: '{keyword}'")

        normalized_keyword = normalize_str_value(keyword)
        normalized_schedule_interval = normalize_schedule_interval(schedule_interval)

        input_dict = {
            "data": data,
            "keyword": normalized_keyword,
            "schedule_interval": normalized_schedule_interval
        }

        validated = validate_interest_by_region_input(input_dict)
        logger.info(f"Normalized data for '{normalized_keyword}'")
        return validated["data"], normalized_schedule_interval, normalized_keyword

    except Exception as e:
        logger.error(f"Error while transforming interest_by_region for '{keyword}': {e}")
        return None, schedule_interval, keyword
