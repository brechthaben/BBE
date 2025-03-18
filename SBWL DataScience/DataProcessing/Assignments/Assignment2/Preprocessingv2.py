import requests
import json
import os
import random
from datetime import datetime

def fetch_data_from_api(url):
    """
    Fetch JSON data from an API endpoint
    
    Args:
        url (str): The API endpoint URL
        
    Returns:
        dict: The JSON response data
    """
    try:
        # Send GET request to the API
        response = requests.get(url)
        
        # Check if the request was successful
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        return data
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def save_to_json_file(data, filename):
    """
    Save data to a JSON file
    
    Args:
        data (dict): The data to save
        filename (str): The filename to save to
    """
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Data successfully saved to {filename}")
    
    except Exception as e:
        print(f"Error saving data: {e}")

def load_json_file(filename):
    """
    Load data from a JSON file
    
    Args:
        filename (str): The filename to load from
        
    Returns:
        dict: The loaded JSON data
    """
    try:
        if not os.path.exists(filename):
            print(f"File {filename} does not exist")
            return None
            
        with open(filename, 'r') as f:
            data = json.load(f)
        print(f"Data successfully loaded from {filename}")
        return data
    
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def filter_data_by_date_range(data, start_year=2013, end_year=2019, date_column="date_rptd"):
    """
    Filter data entries to keep only those with dates between start_year and end_year
    
    Args:
        data (list): List of data entries (dictionaries)
        start_year (int): Start year (inclusive)
        end_year (int): End year (inclusive)
        date_column (str): Name of the date column
        
    Returns:
        list: Filtered data entries
    """
    filtered_data = []
    
    for entry in data:
        if date_column not in entry:
            continue
            
        try:
            # Parse the date from the entry - adjust format as needed
            date_str = entry[date_column]
            
            # The date format may vary, so we'll check the format first
            if "T" in date_str:  # ISO format like "2013-01-01T00:00:00.000"
                date_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            elif "/" in date_str:  # Format like "01/01/2013"
                date_obj = datetime.strptime(date_str, "%m/%d/%Y")
            elif "AM" in date_str or "PM" in date_str:  # Format like "2025 Feb 25 12:00:00 AM"
                date_obj = datetime.strptime(date_str, "%Y %b %d %I:%M:%S %p")
            else:  # Try general ISO format
                date_obj = datetime.fromisoformat(date_str)
                
            # Check if the year is within the range
            if start_year <= date_obj.year <= end_year:
                filtered_data.append(entry)
                
        except (ValueError, TypeError) as e:
            print(f"Error parsing date '{date_str}': {e}")
    
    print(f"Filtered data from {len(data)} to {len(filtered_data)} entries")
    return filtered_data

def random_sample_data(data, sample_size=10000, seed=1):
    """
    Randomly sample a fixed number of entries from the data
    
    Args:
        data (list): List of data entries (dictionaries)
        sample_size (int): Number of entries to sample
        seed (int): Random seed for reproducibility
        
    Returns:
        list: Sampled data entries
    """
    # Set random seed for reproducibility
    random.seed(seed)
    
    # If data has fewer entries than sample_size, return all data
    if len(data) <= sample_size:
        print(f"Data only has {len(data)} entries, returning all entries")
        return data
    
    # Randomly sample entries
    sampled_data = random.sample(data, sample_size)
    print(f"Randomly sampled {sample_size} entries from {len(data)} entries")
    
    return sampled_data

# Example usage
if __name__ == "__main__":
    # Get the current directory where this script is located
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Replace with your API URL
    api_url = "https://data.lacity.org/resource/63jg-8b9z.json"
    
    # Set output file paths in the current directory
    output_file = os.path.join(current_dir, "api_data.json")
    filtered_file = os.path.join(current_dir, "filtered_data_2013_2019.json")
    sampled_file = os.path.join(current_dir, "sampled_data_10k.json")
    final_output_file = os.path.join(current_dir, "final_processed_data.json")
    
    # Check if we already have the data file, if not fetch it
    if not os.path.exists(output_file):
        # Fetch data from API
        json_data = fetch_data_from_api(api_url)
        if json_data:
            # Save raw data to file
            save_to_json_file(json_data, output_file)
    else:
        # Load existing data
        json_data = load_json_file(output_file)
    
    if json_data:
        # Filter data to entries between 2013 and 2019
        filtered_data = filter_data_by_date_range(json_data, 2013, 2019, "date_rptd")
        
        # Save filtered data
        if filtered_data:
            save_to_json_file(filtered_data, filtered_file)
            
            # Sample 10,000 random entries with fixed seed
            sampled_data = random_sample_data(filtered_data, sample_size=10000, seed=1)
            
            # Save sampled data
            if sampled_data:
                save_to_json_file(sampled_data, sampled_file)
                
                # Create a copy of the final sampled data as the final output
                save_to_json_file(sampled_data, final_output_file)
                print(f"Final processed data saved to {final_output_file}")