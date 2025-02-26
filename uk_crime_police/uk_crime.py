#%% step 1
import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Define your API URL and credentials for the PostgreSQL database
url = "https://data.police.uk/api/crimes-street/all-crime"
params = {
    'lat': 52.629729,
    'lng': -1.131592,
    'date': '2023-01'
}

# Fetch data from the API
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
else:
    print("Failed to fetch data, status code:", response.status_code)
    data = []

#%% step 2
# Transform the data into a pandas DataFrame
if data:
    df = pd.json_normalize(data)
    print("Data fetched and transformed into DataFrame:")
    print(df.head())

    
    #%% step 3
    # Define database connection parameters (replace with your actual credentials)
    db_params = {
        'user': '',
        'password': '',
        'host': '',  # or your host IP
        'port': '5432',  # default PostgreSQL port
        'database': ''
    }

    # Connect to the PostgreSQL database
    try:
        engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")
        # Upload the DataFrame to the PostgreSQL table
        df.to_sql('uk_police_crime', engine, if_exists='replace', index=False)
        print("Data uploaded successfully to PostgreSQL.")
    except Exception as e:
        print(f"An error occurred while uploading to PostgreSQL: {e}")
else:
    print("No data found to upload.")

