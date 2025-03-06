import time
import requests
from requests.exceptions import RequestException, ConnectionError, Timeout, HTTPError
from typing import Optional, Dict, Any, Union, Tuple

from utils.logger import request_logger

class RequestError(Exception):
    """Custom exception for request errors with detailed information."""
    def __init__(self, message, status_code=None, url=None, response=None):
        self.message = message
        self.status_code = status_code
        self.url = url
        self.response = response
        super().__init__(self.message)

def make_request(
    url: str, 
    method: str = 'GET',
    params: Dict[str, Any] = None,
    json_data: Dict[str, Any] = None,
    headers: Dict[str, str] = None,
    max_retries: int = 3,
    retry_delay: int = 2,
    timeout: int = 30,
    return_json: bool = True,
    log_response: bool = False
) -> Union[Dict[str, Any], bytes, str]:
    """
    Make an HTTP request with retry logic and error handling.
    
    Args:
        url: The URL to request
        method: HTTP method (GET, POST, etc.)
        params: URL parameters
        json_data: JSON data to send in the request body
        headers: HTTP headers
        max_retries: Maximum number of retries on failure
        retry_delay: Delay between retries in seconds
        timeout: Request timeout in seconds
        return_json: Whether to return JSON data (True) or raw content
        log_response: Whether to log the response content (can be verbose)
        
    Returns:
        Response data as JSON dict, bytes content, or string
        
    Raises:
        RequestError: If the request fails after all retries
    """
    method = method.upper()
    headers = headers or {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    request_logger.info(f"Making {method} request to {url}")
    if params:
        request_logger.debug(f"Request params: {params}")
    if json_data:
        request_logger.debug(f"Request data: {json_data}")
    
    for attempt in range(max_retries):
        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
                headers=headers,
                timeout=timeout
            )
            
            response.raise_for_status()  # Raise exception for 4XX/5XX responses
            
            # Log response info
            request_logger.info(f"Request successful: {response.status_code} {url}")
            
            if log_response:
                try:
                    request_logger.debug(f"Response: {response.text[:500]}...")
                except Exception:
                    request_logger.debug("Could not log response content")
            
            if return_json:
                return response.json()
            else:
                return response.content
                
        except ConnectionError as e:
            request_logger.warning(f"Connection error on attempt {attempt+1}/{max_retries}: {str(e)}")
        except Timeout as e:
            request_logger.warning(f"Request timed out on attempt {attempt+1}/{max_retries}: {str(e)}")
        except HTTPError as e:
            request_logger.error(f"HTTP error on attempt {attempt+1}/{max_retries}: {response.status_code} - {str(e)}")
            # Don't retry on client errors (4xx) except 429 (rate limiting)
            if response.status_code >= 400 and response.status_code < 500 and response.status_code != 429:
                raise RequestError(
                    f"HTTP error: {response.status_code}", 
                    status_code=response.status_code,
                    url=url,
                    response=response
                )
        except ValueError as e:
            # JSON decode error
            request_logger.error(f"JSON decode error on attempt {attempt+1}/{max_retries}: {str(e)}")
            if attempt == max_retries - 1:
                raise RequestError(f"Invalid JSON response: {str(e)}", url=url, response=response)
        except RequestException as e:
            request_logger.error(f"Request error on attempt {attempt+1}/{max_retries}: {str(e)}")
        
        # If we get here, the request failed. Sleep before retrying (if not the last attempt)
        if attempt < max_retries - 1:
            sleep_time = retry_delay * (2 ** attempt)  # Exponential backoff
            request_logger.info(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
    
    # If we get here, all retries failed
    raise RequestError(
        f"Request failed after {max_retries} attempts", 
        url=url
    ) 