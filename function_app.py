import json
import logging
import os
import random
import time
from datetime import datetime

import azure.functions as func
import requests
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup

app = func.FunctionApp()


@app.schedule(schedule="0 0 0 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False)
def ZameenScraper(myTimer: func.TimerRequest) -> None:

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Zameen scraper function triggered at {}'.format(
        datetime.now().isoformat()))

    # Initialize run timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Get configuration from environment variables with defaults
    connection_string = os.environ.get("ZameenBlobStorage")
    if not connection_string:
        logging.error(
            "Missing storage connection string. Please configure ZameenBlobStorage in Application Settings.")
        return

    container_name = "rawdata"
    max_pages = int(os.environ.get("MAX_PAGES", "100"))
    scraper_delay = float(os.environ.get("SCRAPER_DELAY", "2.5"))

    # Set up blob storage client
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string)
        container_client = blob_service_client.get_container_client(
            container_name)

        # Create container if it doesn't exist
        try:
            container_client.create_container()
            logging.info(f"Container {container_name} created")
        except Exception as e:
            logging.info(
                f"Container {container_name} already exists or error: {str(e)}")
    except Exception as e:
        logging.error(f"Failed to connect to Blob Storage: {str(e)}")
        return


    # List of cities to scrape
    cities = [
        {'id': 1, 'name': 'Lahore'},
        {'id': 2, 'name': 'Karachi'},
        {'id': 3, 'name': 'Islamabad'},
        {'id': 15, 'name': 'Multan'},
        {'id': 16, 'name': 'Faisalabad'},
        {'id': 17, 'name': 'Peshawar'},
        {'id': 18, 'name': 'Quetta'},
        {'id': 41, 'name': 'Rawalpindi'},
        {'id': 36, 'name': 'Murree'},
        {'id': 327, 'name': 'Gujranwala'},
        {'id': 1233, 'name': 'Attock'},
    ]

    # Create a run metadata file
    run_metadata = {
        "run_id": timestamp,
        "start_time": datetime.now().isoformat(),
        "cities": [city["name"] for city in cities],
        "max_pages_per_city": max_pages,
        "scraper_delay": scraper_delay,
        "status": "in_progress"
    }

    # Upload run metadata
    metadata_blob_name = f"{timestamp}/run_metadata.json"
    metadata_blob_client = container_client.get_blob_client(metadata_blob_name)
    metadata_blob_client.upload_blob(json.dumps(run_metadata), overwrite=True)

    # Create CSV header file for this run
    csv_header = "city|location|price|bedrooms|baths|size|timestamp|listing_id\n"
    header_blob_name = f"{timestamp}/header.csv"
    header_blob_client = container_client.get_blob_client(header_blob_name)
    header_blob_client.upload_blob(csv_header, overwrite=True)

    total_listings = 0
    city_stats = {}

    # Process each city
    for city in cities:
        logging.info(f"Processing city: {city['name']}")
        city_start_time = datetime.now()

        try:

            # Run the scraper with rate limiting
            city_data = scrap(
                f"{city.get('name')}-{city.get('id')}",
                max_pages,
                delay=scraper_delay
            )

            # If no data was returned, log and continue
            if not city_data:
                logging.warning(f"No data returned for {city['name']}")
                city_stats[city['name']] = {
                    "status": "no_data",
                    "listings_count": 0,
                    "duration_seconds": (datetime.now() - city_start_time).total_seconds()
                }
                continue

            # Count listings
            listings_count = len(city_data)
            total_listings += listings_count

            # Save JSON data to blob storage
            city_json_blob_name = f"{timestamp}/{city['name'].lower()}_data.json"
            city_json_blob_client = container_client.get_blob_client(
                city_json_blob_name)

            # Add timestamp to each listing
            current_time = datetime.now().isoformat()
            for listing in city_data:
                listing["timestamp"] = current_time
                listing["city"] = city['name']

            city_json_blob_client.upload_blob(
                json.dumps(city_data), overwrite=True)

            # Also save as CSV for compatibility with downstream processes
            csv_content = ""
            for listing in city_data:
                listing_id = f"{city['name']}_{listing.get('location', '')}_{listing.get('price', 0)}"
                csv_content += f"{city['name']}|{listing.get('location', '')}|{listing.get('price', 0)}|"
                csv_content += f"{listing.get('bedrooms', 0)}|{listing.get('baths', 0)}|"
                csv_content += f"{listing.get('size', 0)}|{current_time}|{listing_id}\n"

            city_csv_blob_name = f"{timestamp}/{city['name'].lower()}_data.csv"
            city_csv_blob_client = container_client.get_blob_client(
                city_csv_blob_name)
            city_csv_blob_client.upload_blob(csv_content, overwrite=True)

            # Update city stats
            city_stats[city['name']] = {
                "status": "success",
                "listings_count": listings_count,
                "duration_seconds": (datetime.now() - city_start_time).total_seconds()
            }

            logging.info(
                f"Uploaded {listings_count} listings for {city['name']} to blob storage")

        except Exception as e:
            logging.error(f"Error scraping {city['name']}: {str(e)}")
            city_stats[city['name']] = {
                "status": "error",
                "error_message": str(e),
                "duration_seconds": (datetime.now() - city_start_time).total_seconds()
            }

    # Update run metadata with completion information
    run_metadata["end_time"] = datetime.now().isoformat()
    run_metadata["status"] = "completed"
    run_metadata["total_listings"] = total_listings
    run_metadata["city_stats"] = city_stats
    run_metadata["duration_seconds"] = (datetime.now(
    ) - datetime.fromisoformat(run_metadata["start_time"])).total_seconds()

    # Upload updated metadata
    metadata_blob_client.upload_blob(json.dumps(run_metadata), overwrite=True)

    logging.info(
        f'Zameen.com scraper completed successfully. Total listings: {total_listings}')

def convert_price(price):
    """
    Convert crore, lakhs, millions and Thousand into numbers

    :param price: str
    :return: float
    """
    if not price:
        return 0

    # Clean the price string
    price = price.strip()

    try:
        if price.endswith('Crore'):
            return round(float(price[:-5].strip().replace(",", "")) * 10000000)
        elif price.endswith('Lakh'):
            return round(float(price[:-4].strip().replace(",", "")) * 100000)
        elif price.endswith('Million'):
            return round(float(price[:-7].strip().replace(",", "")) * 1000000)
        elif price.endswith('Arab'):
            return round(float(price[:-4].strip().replace(",", "")) * 1000000000)
        elif price.endswith('Thousand'):
            return round(float(price[:-8].strip().replace(",", "")) * 1000)
        else:
            # Handle numeric price with commas
            return round(float(price.replace(",", "")))
    except (ValueError, TypeError) as e:
        logging.warning(f"Error converting price '{price}': {str(e)}")
        return 0


def convert_size(size):
    """
    Convert kanal merla into sqft

    :param size: str
    :return: float
    """
    if not size:
        return 0

    # Clean the size string
    size = size.strip()

    try:
        if size.endswith('Marla'):
            return round(float(size[:-5].strip().replace(",", "")) * 225)
        elif size.endswith('Kanal'):
            return round(float(size[:-5].strip().replace(",", "")) * 4500)
        elif size.endswith('Sq. Yd.'):
            return round(float(size[:-7].strip().replace(",", "")) * 9)
        elif size.endswith('Sq. Ft.'):
            return round(float(size[:-7].strip().replace(",", "")))
        else:
            # Assume square feet if no unit specified
            return round(float(size.replace(",", "")))
    except (ValueError, TypeError) as e:
        logging.warning(f"Error converting size '{size}': {str(e)}")
        return 0


def text(tag, datatype="str"):
    """
    This function will return the text of the tag.

    :param tag: tag object
    :param datatype: num or str or price, size
    :return: price in number or string
    """
    if tag is None and datatype == "num":
        return 0
    if datatype == "num":
        try:
            return int(tag.text.strip())
        except ValueError:
            return 0
    if tag is None and datatype == "str":
        return ""
    if datatype == "str":
        return tag.text.strip()
    if tag is None and datatype == "price":
        return 0.0
    if datatype == "price":
        return convert_price(tag.text.strip())
    if tag is None and datatype == "size":
        return 0.0
    if datatype == "size":
        return convert_size(tag.text.strip())


def extract_additional_features(house):
    """
    Extract additional features from the house listing
    
    :param house: BeautifulSoup object representing a house listing
    :return: dict of additional features
    """
    try:
        # Try to extract property type
        property_type_element = house.select_one(
            "div[aria-label='Property Type']")
        property_type = text(
            property_type_element) if property_type_element else ""

        # Try to extract listing date
        date_element = house.select_one("div[aria-label='Date Added']")
        date_added = text(date_element) if date_element else ""

        # Try to extract listing ID if available
        id_element = house.select_one("div[aria-label='Reference']")
        listing_id = text(id_element) if id_element else ""

        return {
            "property_type": property_type,
            "date_added": date_added,
            "listing_id": listing_id
        }
    except Exception as e:
        logging.warning(f"Error extracting additional features: {str(e)}")
        return {
            "property_type": "",
            "date_added": "",
            "listing_id": ""
        }


def scrap(city, pages_range, delay=2.5):
    """
    This function will scrape the zameen.com website and
    return the list of houses information with rate limiting and error handling
    
    :param city: str
    :param pages_range: int
    :param delay: float - seconds to wait between requests
    :param user_agent: str - user agent to use for requests
    :return: list
    """
    house_info = []
    # user_agents = [
    #     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    #     'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    # ]

    # Count consecutive errors
    consecutive_errors = 0
    max_consecutive_errors = 3

    for page_number in range(1, pages_range+1):
        # Stop if we've had too many consecutive errors
        if consecutive_errors >= max_consecutive_errors:
            logging.error(
                f"Too many consecutive errors ({consecutive_errors}). Stopping scraping for {city}")
            break

        url = f'https://www.zameen.com/Homes/{city}-{page_number}.html'
        logging.info(f"Scraping: {url} (Page {page_number}/{pages_range})")

        try:
            # Add randomized delay between requests (ethical scraping)
            jitter = random.uniform(0.8, 1.2)
            time.sleep(delay * jitter)


            # Make the request with timeout
            response = requests.get(url,timeout=30)
            response.raise_for_status()  # Raise exception for 4XX/5XX status

            # Check if the response is a valid HTML page
            if not response.text.strip() or "<html" not in response.text.lower():
                logging.warning(f"Invalid HTML response from {url}")
                consecutive_errors += 1
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            house_list = soup.select("main > div > div > div > div > ul > li")

            # If no houses found using the main selector, try an alternative
            if not house_list:
                house_list = soup.select("div.listingTile")

            # If still no houses, check if we're on the last page or wrong selector
            if not house_list:
                logging.info(
                    f"No houses found on page {page_number} for {city}. Selector may be outdated or last page reached.")
                break

            # Store length of previous house info list
            prev_len = len(house_info)

            # Process each house listing
            for house in house_list:
                try:
                    baths = house.select_one("span[aria-label='Baths']")
                    beds = house.select_one("span[aria-label='Beds']")
                    location = house.select_one("div[aria-label='Location']")
                    price = house.select_one("span[aria-label='Price']")
                    size = house.select_one(
                        "div[title]>div > div > span:nth-child(1)")

                    if price:
                        if size is None:
                            size = location.parent.select_one(
                                "div:nth-child(2) > div > span:nth-child(3)")

                        # Extract additional features
                        additional_features = extract_additional_features(
                            house)

                        # Create the listing data
                        listing_data = {
                            "location": text(location),
                            "price": text(price, datatype="price"),
                            "bedrooms": text(beds, datatype="num"),
                            "baths": text(baths, datatype="num"),
                            "size": text(size, datatype="size"),
                            "property_type": additional_features["property_type"],
                            "date_added": additional_features["date_added"],
                            "original_listing_id": additional_features["listing_id"],
                            "scrape_date": datetime.now().isoformat()
                        }

                        house_info.append(listing_data)
                except Exception as e:
                    logging.warning(
                        f"Error processing house listing: {str(e)}")
                    continue

            # Reset consecutive error counter on success
            consecutive_errors = 0

            # Check if we should stop pagination
            if len(house_info) == prev_len:
                logging.info(
                    f"No more results found after page {page_number} for {city}")
                break

        except requests.exceptions.RequestException as e:
            logging.error(f"Request error on {url}: {str(e)}")
            consecutive_errors += 1
            # Wait longer after an error
            time.sleep(delay * 3)
            continue
        except Exception as e:
            logging.error(f"Unexpected error on {url}: {str(e)}")
            consecutive_errors += 1
            time.sleep(delay * 3)
            continue

    logging.info(
        f"Completed scraping {city}. Total listings: {len(house_info)}")
    return house_info
