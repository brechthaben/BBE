import json
import random
from datetime import datetime
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_data():
    try:
        # Print current working directory
        current_dir = os.getcwd()
        logger.info(f"Current working directory: {current_dir}")

        # Get the directory where the script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        logger.info(f"Script directory: {script_dir}")

        # Construct absolute path to input file
        input_file = os.path.join(script_dir, "raw_data.json")
        output_file = os.path.join(script_dir, "processed_data.json")

        logger.info(f"Attempting to read from: {input_file}")

        # Load raw data
        with open(input_file, 'r') as f:
            raw_data = json.load(f)
        logger.info(f"Loaded {len(raw_data)} entries from raw_data.json")

        # Filter data for years 2013-2019
        filtered_data = []
        for entry in raw_data:
            try:
                date_str = entry['date_rptd'].split('.')[0]  # Remove milliseconds if present
                date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
                if 2013 <= date.year <= 2019:
                    filtered_data.append(entry)
            except (KeyError, ValueError) as e:
                continue

        logger.info(f"Filtered to {len(filtered_data)} entries between 2013-2019")

        # Sample 10000 random entries
        if len(filtered_data) > 10000:
            random.seed(1)  # For reproducibility
            sampled_data = random.sample(filtered_data, 10000)
        else:
            sampled_data = filtered_data
            logger.warning(f"Less than 10000 entries available. Using all {len(filtered_data)} entries.")

        # Save processed data
        with open(output_file, 'w') as f:
            json.dump(sampled_data, f, indent=4)
        
        logger.info(f"Successfully saved {len(sampled_data)} entries to {output_file}")

    except Exception as e:
        logger.error(f"Error processing data: {e}")
        # Print more detailed error information
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    process_data()