import requests
import json
import os
import random
from datetime import datetime
import time
from typing import List, Dict, Optional, Union
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, api_url: str, output_dir: str):
        """
        Initialize DataProcessor with API URL and output directory
        
        Args:
            api_url (str): The API endpoint URL
            output_dir (str): Directory for output files
        """
        self.api_url = api_url
        self.output_dir = output_dir
        self.ensure_output_directory()

    def ensure_output_directory(self):
        """Create output directory if it doesn't exist"""
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch_data_with_pagination(self, limit: int = 1000, max_retries: int = 3, 
                                 retry_delay: int = 1) -> Optional[List[Dict]]:
        """
        Fetch all data from API using pagination
        
        Args:
            limit (int): Number of records per request
            max_retries (int): Maximum number of retry attempts
            retry_delay (int): Delay between retries in seconds
            
        Returns:
            Optional[List[Dict]]: List of data entries or None if failed
        """
        try:
            all_data = []
            offset = 0
            
            while True:
                retries = 0
                while retries < max_retries:
                    try:
                        paginated_url = f"{self.api_url}?$limit={limit}&$offset={offset}"
                        response = requests.get(paginated_url)
                        response.raise_for_status()
                        
                        batch_data = response.json()
                        if not batch_data:
                            logger.info("No more data to fetch")
                            return all_data
                            
                        all_data.extend(batch_data)
                        logger.info(f"Fetched {len(batch_data)} records. Total: {len(all_data)}")
                        
                        if len(batch_data) < limit:
                            return all_data
                            
                        offset += limit
                        time.sleep(0.1)  # Rate limiting
                        break
                        
                    except requests.exceptions.RequestException as e:
                        retries += 1
                        if retries >= max_retries:
                            logger.error(f"Failed after {max_retries} retries: {e}")
                            return None
                        logger.warning(f"Request failed, retry {retries}/{max_retries}")
                        time.sleep(retry_delay)
                
        except Exception as e:
            logger.error(f"Unexpected error during data fetch: {e}")
            return None

    def filter_by_date_range(self, data: List[Dict], start_year: int, 
                           end_year: int, date_column: str) -> List[Dict]:
        """
        Filter data by date range
        
        Args:
            data (List[Dict]): Data to filter
            start_year (int): Start year (inclusive)
            end_year (int): End year (inclusive)
            date_column (str): Name of date column
            
        Returns:
            List[Dict]: Filtered data
        """
        filtered_data = []
        date_formats = [
            "%Y-%m-%dT%H:%M:%S.%f",  # ISO format
            "%m/%d/%Y",              # MM/DD/YYYY
            "%Y %b %d %I:%M:%S %p"   # Year Month Day HH:MM:SS AM/PM
        ]

        for entry in data:
            if date_column not in entry:
                continue

            date_str = entry[date_column]
            parsed_date = None

            for date_format in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str.split('.')[0], date_format)
                    break
                except ValueError:
                    continue

            if parsed_date and start_year <= parsed_date.year <= end_year:
                filtered_data.append(entry)

        logger.info(f"Filtered {len(data)} entries to {len(filtered_data)} entries")
        return filtered_data

    def random_sample(self, data: List[Dict], sample_size: int, seed: int) -> List[Dict]:
        """
        Take a random sample of data entries
        
        Args:
            data (List[Dict]): Data to sample from
            sample_size (int): Number of samples to take
            seed (int): Random seed
            
        Returns:
            List[Dict]: Sampled data
        """
        random.seed(seed)
        if len(data) <= sample_size:
            logger.info(f"Data size ({len(data)}) <= sample size ({sample_size})")
            return data
            
        sampled_data = random.sample(data, sample_size)
        logger.info(f"Sampled {sample_size} entries from {len(data)} entries")
        return sampled_data

    def save_json(self, data: Union[List, Dict], filename: str) -> bool:
        """
        Save data to JSON file
        
        Args:
            data (Union[List, Dict]): Data to save
            filename (str): Output filename
            
        Returns:
            bool: Success status
        """
        try:
            filepath = os.path.join(self.output_dir, filename)
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=4)
            logger.info(f"Data saved to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            return False

    def load_json(self, filename: str) -> Optional[Union[List, Dict]]:
        """
        Load data from JSON file
        
        Args:
            filename (str): File to load
            
        Returns:
            Optional[Union[List, Dict]]: Loaded data or None if failed
        """
        try:
            filepath = os.path.join(self.output_dir, filename)
            if not os.path.exists(filepath):
                logger.warning(f"File not found: {filepath}")
                return None
                
            with open(filepath, 'r') as f:
                data = json.load(f)
            logger.info(f"Data loaded from {filepath}")
            return data
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return None

def main():
    # Configuration
    API_URL = "https://data.lacity.org/resource/63jg-8b9z.json"
    OUTPUT_DIR = "processed_data"
    
    # Initialize processor
    processor = DataProcessor(API_URL, OUTPUT_DIR)
    
    # File names
    RAW_DATA_FILE = "raw_data.json"
    FILTERED_DATA_FILE = "filtered_data.json"
    FINAL_DATA_FILE = "final_data.json"
    
    # Process data
    try:
        # Check for existing raw data
        raw_data = processor.load_json(RAW_DATA_FILE)
        
        if not raw_data:
            # Fetch new data
            raw_data = processor.fetch_data_with_pagination()
            if raw_data:
                processor.save_json(raw_data, RAW_DATA_FILE)
        
        if raw_data:
            # Filter by date range
            filtered_data = processor.filter_by_date_range(
                raw_data, 
                start_year=2013, 
                end_year=2019, 
                date_column="date_rptd"
            )
            processor.save_json(filtered_data, FILTERED_DATA_FILE)
            
            # Sample data
            final_data = processor.random_sample(
                filtered_data,
                sample_size=10000,
                seed=1
            )
            processor.save_json(final_data, FINAL_DATA_FILE)
            
            logger.info("Data processing completed successfully")
            
    except Exception as e:
        logger.error(f"Error in main processing: {e}")

if __name__ == "__main__":
    main()