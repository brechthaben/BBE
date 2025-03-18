import requests
import json

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

# Example usage
if __name__ == "__main__":
    # Replace with your API URL
    api_url = "https://data.lacity.org/resource/63jg-8b9z.json"
    
    # Fetch data from API
    json_data = fetch_data_from_api(api_url)
    
    if json_data:
        # Work with the data
        print(f"Retrieved {len(json_data)} items")
        
        # Example: access specific data
        # if 'results' in json_data:
        #     for item in json_data['results']:
        #         print(item['name'])
        
        # Save data to file if needed
        save_to_json_file(json_data, "api_data.json")