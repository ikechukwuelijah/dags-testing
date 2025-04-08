#%%
import requests
import pandas as pd

# Define the API URL
url = "https://health.gov/myhealthfinder/api/v3/myhealthfinder.json?lang=en&age=38&sex=male&tobaccoUse=1&sexuallyActive=1&pregnant=0"

# Make a GET request to the API
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()
    
    # Print out the structure for debugging
    print(data["Result"])  # Inspect the structure before extracting

# %%
