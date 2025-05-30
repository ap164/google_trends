import time
import logging

logger = logging.getLogger(__name__)

def extract_interest_over_time(pytrends, keyword, timeframe, category, geo, gprop):
    try:
        time.sleep(50) 
        pytrends.build_payload([keyword], cat=category, timeframe=timeframe, geo=geo, gprop=gprop)
        data = pytrends.interest_over_time()
        if data is not None and not data.empty:
            logger.info(f"Fetched interest_over_time data for '{keyword}'.")
            return data
        else:
            logger.warning(f"No interest_over_time data for '{keyword}'.")
            return "Google Trends returned an empty response"
    except Exception as e:
        error_msg = str(e)
        if "429" in error_msg:  
            logger.warning(f"Error 429: Too many requests, will retry for '{keyword}'.")
            return "429: Too many requests"
        else:
            logger.warning(f"Error while fetching interest_over_time for '{keyword}': {error_msg}")
            return error_msg

def extract_interest_by_region(pytrends, keyword, category, geo, gprop):
    try:
        time.sleep(50) 
        pytrends.build_payload([keyword], cat=category, geo=geo, gprop=gprop)
        resolution = 'COUNTRY' if geo == "" else 'REGION'
        data = pytrends.interest_by_region(resolution=resolution)
        
        if data is not None and not data.empty:
            logger.info(f"Fetched interest_by_region data for '{keyword}'.")
            return data
        else:
            logger.warning(f"No interest_by_region data for '{keyword}'.")
            return "Google Trends returned an empty response"
    except Exception as e:
        error_msg = str(e)
        if "429" in error_msg:  
            logger.warning(f"Error 429: Too many requests, will retry for '{keyword}'.")
            return "429: Too many requests"
        else:
            logger.warning(f"Error while fetching interest_by_region for '{keyword}': {error_msg}")
            return error_msg
