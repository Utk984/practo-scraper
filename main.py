import sys
import time
import traceback
from tqdm import tqdm

import config
from db.insert_db import insert_main_data, insert_relation_data, DatabaseError
from db.init_db import init_db
from utils.logger import app_logger, request_logger
from parser.establishment import parse_establishment_doctor_relation, parse_establishment_data
from parser.doctor import parse_doctor_establishment_relation, parse_doctors_data
from utils.http import make_request, RequestError


def parse_and_store_relation(practo_id, json_response, html_content, result_type):
    """Process and store relationship data between entities.
    
    Args:
        practo_id (str): The Practo ID of the main entity
        json_response (dict): The JSON response data
        html_content (bytes): The HTML content from the page
        result_type (str): Type of entity ('doctor', 'hospital', or 'clinic')
        
    Returns:
        bool: True if successful, False otherwise
    """
    mapping = {
        "hospital": parse_establishment_doctor_relation,
        "clinic": parse_establishment_doctor_relation,
        "doctor": parse_doctor_establishment_relation,
    }

    try:
        app_logger.info(f"Parsing and storing relation data for {result_type} with ID {practo_id}")
        data, doctor_count, bed_count, amb_count = mapping[result_type](
            practo_id, json_response, html_content
        )
        insert_relation_data(data, doctor_count, bed_count, amb_count, practo_id, result_type)
        app_logger.info(f"Successfully processed relation data for {result_type} with ID {practo_id}")
        return True
    except Exception as e:
        app_logger.error(f"Error in parse_and_store_relation for {result_type} with ID {practo_id}: {str(e)}")
        return False


def parse_and_store_main(response, result_type):
    """Process and store main entity data.
    
    Args:
        response (dict): The JSON response data
        result_type (str): Type of entity ('doctor', 'hospital', or 'clinic')
        
    Returns:
        list: List of entity profiles with ID, slug, and URL
        
    Raises:
        ValueError: If the response format is unexpected
    """
    mapping = {
        "hospital": parse_establishment_data,
        "clinic": parse_establishment_data,
        "doctor": parse_doctors_data,
    }

    try:
        app_logger.info(f"Parsing and storing main data for {result_type}")
        data, profile, query = mapping[result_type](response)
        
        if not data:
            app_logger.warning(f"No data found for {result_type} in response")
            return []
        
        insert_main_data(data, query)
        app_logger.info(f"Successfully processed main data for {result_type}, found {len(profile)} entities")
        return profile
    except (KeyError, TypeError) as e:
        app_logger.error(f"Data structure error in parse_and_store_main for {result_type}: {str(e)}")
        raise ValueError(f"Unexpected response format: {str(e)}")
    except Exception as e:
        app_logger.error(f"Error in parse_and_store_main for {result_type}: {str(e)}")
        raise


def main():
    """Main function to process URLs and extract data."""
    app_logger.info("Starting Practo data processing")
    
    # Initialize database tables if they don't exist
    try:
        init_db()
    except Exception as e:
        app_logger.error(f"Failed to initialize database: {str(e)}")
        sys.exit(1)
    
    # Read URLs
    try:
        with open("urls.txt", "r") as file:
            urls = [url.strip() for url in file.readlines() if url.strip()]
        app_logger.info(f"Loaded {len(urls)} URLs from urls.txt")
    except FileNotFoundError:
        app_logger.error("urls.txt file not found")
        sys.exit(1)
    except Exception as e:
        app_logger.error(f"Error reading URLs file: {str(e)}")
        sys.exit(1)

    # Setup profile URL mapping
    mapping = {
        "hospital": config.ESTABLISHMENT_PROFILE_URL,
        "clinic": config.ESTABLISHMENT_PROFILE_URL,
        "doctor": config.DOCTOR_PROFILE_URL,
    }

    # Process each URL
    for url_index, link in enumerate(urls):
        try:
            app_logger.info(f"Processing URL {url_index+1}/{len(urls)}: {link[:50]}...")
            
            # Get initial response
            try:
                response = make_request(link)
            except RequestError as e:
                app_logger.error(f"Failed to fetch URL {link}: {str(e)}")
                continue
                
            # Determine result count based on URL type
            try:
                if "DOCTOR_SEARCH" in link:
                    count = int(response.get("listing_data", {}).get("doctors_found", 0))
                    result_type = "doctor"
                else:
                    count = int(response.get("form", {}).get("total_results", 0))
                    result_type = response.get("form", {}).get("results_type", "unknown")
                
                app_logger.info(f"Found {count} results of type {result_type}")
                
                if count == 0:
                    app_logger.warning(f"No results found for URL: {link}")
                    continue
            except (KeyError, TypeError, ValueError) as e:
                app_logger.error(f"Error parsing result count: {str(e)}")
                continue
                
            # Process each page of results
            success_count = 0
            error_count = 0
            total_pages = (count // 10) + 1
            
            for page in tqdm(range(1, total_pages + 1), desc=f"Processing {result_type} pages"):
                page_url = link.replace("page=1", f"page={page}")
                app_logger.debug(f"Processing page {page}/{total_pages}: {page_url}")
                
                # Add delay to avoid rate limiting
                if page > 1 and page % 5 == 0:
                    time.sleep(3)
                
                try:
                    # Get page data
                    try:
                        page_response = make_request(page_url)
                    except RequestError as e:
                        app_logger.error(f"Failed to fetch page {page}: {str(e)}")
                        error_count += 1
                        continue
                    
                    # Determine entity type
                    result_type = page_response.get("form", {}).get("results_type")
                    if not result_type:
                        app_logger.warning(f"Could not determine entity type from page {page}")
                        continue
                    
                    # Process main entity data
                    profile = parse_and_store_main(page_response, result_type)
                    
                    # Process relationships for each entity
                    for practo_id, slug, url in profile:
                        try:
                            # Get profile data
                            profile_response = make_request(mapping[result_type].format(slug=slug))
                            
                            # Get HTML content
                            html_content = make_request(url, return_json=False)
                            
                            # Process and store relation data
                            if parse_and_store_relation(practo_id, profile_response, html_content, result_type):
                                success_count += 1
                            else:
                                error_count += 1
                                
                        except RequestError as e:
                            app_logger.error(f"Request error for {result_type} {practo_id}: {str(e)}")
                            error_count += 1
                        except Exception as e:
                            app_logger.error(f"Error processing {result_type} {practo_id}: {str(e)}")
                            error_count += 1
                
                except DatabaseError as e:
                    app_logger.error(f"Database error on page {page}: {str(e)}")
                    error_count += 1
                except Exception as e:
                    app_logger.error(f"Unexpected error on page {page}: {str(e)}")
                    app_logger.debug(traceback.format_exc())
                    error_count += 1
            
            app_logger.info(f"Completed URL {url_index+1}/{len(urls)}: Processed {success_count} entities with {error_count} errors")
            
        except Exception as e:
            app_logger.error(f"Failed to process URL {link}: {str(e)}")
            app_logger.debug(traceback.format_exc())
    
    app_logger.info("Practo data processing completed")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        app_logger.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        app_logger.critical(f"Unhandled exception in main process: {str(e)}")
        app_logger.debug(traceback.format_exc())
        sys.exit(1)
