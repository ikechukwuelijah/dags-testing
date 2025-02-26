import requests
import pandas as pd
import psycopg2
import time

# PostgreSQL Connection Details
DB_HOST = "89.40.0.150"
DB_PORT = "5432"
DB_NAME = "dwh"
DB_USER = "ikeengr"
DB_PASSWORD = "DataEngineer247"

# FBI API Base URL
API_URL = "https://api.fbi.gov/wanted/v1/list"

def get_total_records():
    """Fetches the total number of records available in the FBI Wanted API."""
    response = requests.get(API_URL)
    if response.status_code == 200:
        return response.json().get("total", 0)
    else:
        raise Exception(f"Failed to fetch total records. Status Code: {response.status_code}")

def fetch_all_records():
    """Fetches all records from the FBI API using pagination."""
    all_records = []
    page = 1
    page_size = 50  # Adjust as needed

    total_records = get_total_records()
    print(f"Total Records Available: {total_records}")

    while len(all_records) < total_records:
        print(f"Fetching page {page}...")
        response = requests.get(f"{API_URL}?pageSize={page_size}&page={page}")

        if response.status_code != 200:
            print(f"Error fetching page {page}. Status: {response.status_code}")
            break

        data = response.json().get("items", [])
        all_records.extend(data)

        if not data:
            break

        page += 1
        time.sleep(1)  # Avoid API rate limits

    print(f"Total Records Fetched: {len(all_records)}")
    return all_records

def transform_data(data):
    """Transforms raw JSON data into a structured Pandas DataFrame."""
    df = pd.DataFrame(data)

    # Keep only relevant columns
    columns_needed = ["title", "sex", "race", "dates_of_birth_used", "nationality", "reward_text", "url"]
    df = df[columns_needed]

    # Handle missing values
    df.fillna("Unknown", inplace=True)

    return df

def load_to_postgres(df):
    """Loads the transformed data into PostgreSQL table `fbi_list`."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        for _, row in df.iterrows():
            try:
                cursor.execute(
                    """
                    INSERT INTO fbi_list (title, sex, race, dates_of_birth_used, nationality, reward_text, url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING;
                    """,
                    (row["title"], row["sex"], row["race"], row["dates_of_birth_used"],
                     row["nationality"], row["reward_text"], row["url"])
                )
            except Exception as e:
                print(f"Error inserting row: {e}")
                conn.rollback()
            else:
                conn.commit()

        cursor.close()
        conn.close()
        print(f"Successfully inserted {len(df)} records into PostgreSQL")
    except Exception as e:
        print(f"Database connection failed: {e}")

if __name__ == "__main__":
    wanted_list = fetch_all_records()
    transformed_df = transform_data(wanted_list)
    load_to_postgres(transformed_df)
