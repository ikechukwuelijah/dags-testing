
#%%
import requests
import pandas as pd

# Base API endpoint
base_url = "https://clinicaltrials.gov/api/v2/studies"

# Query parameters
params = {
    "query.cond": "COVID-19",
    "query.term": "treatment",
    "format": "json",
    "pageSize": 100  # Max allowed per page
}

headers = {
    "User-Agent": "Mozilla/5.0"
}

all_studies = []
next_page_token = None

try:
    while True:
        if next_page_token:
            params["pageToken"] = next_page_token
        else:
            params.pop("pageToken", None)

        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()

        studies = data.get("studies", [])
        for study in studies:
            protocol = study.get("protocolSection", {})
            interventions = protocol.get("interventionModule", {}).get("interventions", [])
            intervention_names = [i.get("name") for i in interventions]

            all_studies.append({
                "NCT Number": protocol.get("identificationModule", {}).get("nctId"),
                "Title": protocol.get("identificationModule", {}).get("briefTitle"),
                "Status": protocol.get("statusModule", {}).get("overallStatus"),
                "Interventions": intervention_names
            })

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break  # No more pages

    # Convert to DataFrame
    df = pd.DataFrame(all_studies)
    print(f"Total studies fetched: {len(df)}")
    print(df.head())

except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
except ValueError as e:
    print(f"JSON decode error: {e}")
    print(f"Response content: {response.text}")

# %%
import psycopg2
from psycopg2.extras import execute_values

# Database connection parameters
db_config = {
    "dbname": "dwh",
    "user": "ikeengr",
    "password": "DataEngineer247",
    "host": "89.40.0.150",
    "port": "5432"
}

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    print("Connected to PostgreSQL database.")

    # Create table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS covid_clinical_trials (
        nct_number TEXT PRIMARY KEY,
        title TEXT,
        status TEXT,
        interventions TEXT[]
    );
    """
    cursor.execute(create_table_query)

    # Prepare data for insertion
    values = [
        (
            row["NCT Number"],
            row["Title"],
            row["Status"],
            row["Interventions"]
        )
        for index, row in df.iterrows()
    ]

    insert_query = """
    INSERT INTO covid_clinical_trials (nct_number, title, status, interventions)
    VALUES %s
    ON CONFLICT (nct_number) DO NOTHING;
    """
    execute_values(cursor, insert_query, values)

    conn.commit()
    print(f"{len(values)} records inserted into 'covid_clinical_trials' table.")

except Exception as e:
    print(f"Database error: {e}")
finally:
    if conn:
        cursor.close()
        conn.close()
        print("Database connection closed.")

# %%
