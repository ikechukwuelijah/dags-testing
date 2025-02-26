import requests

# Define the API endpoint and parameters
url = "https://api.opencorporates.com/v0.4/companies/search"
params = {
    'q': 'Tesla'
}

# Send a GET request to the API
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    # Print the JSON response
    print(response.json())
else:
    print(f"Error: {response.status_code}")
