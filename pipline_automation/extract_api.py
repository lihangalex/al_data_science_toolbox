import pandas as pd
import requests

def extract_api(api_url, headers):
    print(f"Extracting data from API: {api_url}")
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status() # raise HTTPError for bad responses
        if response.headers.get("Content-Type") == "Application/json":
            return pd.jason_normalize(response.json())
        else:
            print(f"Unsupported content type: {response.headers.get('Content-Type')}")
            return pd.DataFrame()
    except requests.exceptions.Timeout:
        print("Error: API request timed out.")
        return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        print(f"Error during API extraction: {e}")
        return pd.DataFrame()

# determine the response format
response = requests.get("https://jsonplaceholder.typicode.com/users")
print(response.headers["Content-Type"])