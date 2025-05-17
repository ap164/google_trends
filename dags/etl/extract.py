import time
import logging

logger = logging.getLogger(__name__)

def extract_interest_over_time(pytrends, keyword, timeframe, category, geo, gprop):
    """Fetches interest over time data."""
    try:
        time.sleep(50)  # Wait time between requests
        pytrends.build_payload([keyword], cat=category, timeframe=timeframe, geo=geo, gprop=gprop)
        data = pytrends.interest_over_time()
        if data is not None and not data.empty:
            logger.info(f"Fetched interest_over_time data for '{keyword}'.")
            return data
        else:
            logger.warning(f"No interest_over_time data for '{keyword}'.")
            return None
    except Exception as e:
        # Check for 429 error in the message
        if "429" in str(e):  
            logger.error(f"Error 429: Too many requests, will retry for '{keyword}'.")
            return keyword  # Return only keyword in case of 429 error
        else:
            logger.error(f"Error while fetching interest_over_time for '{keyword}': {e}")
            return None

def extract_interest_by_region(pytrends, keyword, category, geo, gprop):
    """Fetches interest by region data."""
    try:
        time.sleep(50)  # Wait time between requests
        pytrends.build_payload([keyword], cat=category, geo=geo, gprop=gprop)
        data = pytrends.interest_by_region(resolution='COUNTRY')
        if data is not None and not data.empty:
            logger.info(f"Fetched interest_by_region data for '{keyword}'.")
            return data
        else:
            logger.warning(f"No interest_by_region data for '{keyword}'.")
            return None
    except Exception as e:
        # Check for 429 error in the message
        if "429" in str(e):  
            logger.error(f"Error 429: Too many requests, will retry for '{keyword}'.")
            return keyword  # Return only keyword in case of 429 error
        else:
            logger.error(f"Error while fetching interest_by_region for '{keyword}': {e}")
            return None
