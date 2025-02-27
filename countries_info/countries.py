import requests
import pandas as pd
import psycopg2
from psycopg2 import sql

# PostgreSQL database credentials
DB_NAME = ''
DB_USER = ''
DB_PASSWORD = ''
DB_HOST = ''
DB_PORT = '5432'

# Function to fetch data from REST Countries API
def fetch_all_countries():
    url = 'https://restcountries.com/v3.1/all'
    response = requests.get(url)
    return response.json()

# Transforming data into a DataFrame
def transform_data(data):
    countries = []
    for country in data:
        country_info = {
            'Name': country.get('name', {}).get('common', 'N/A'),
            'Capital': country.get('capital', ['N/A'])[0],
            'Region': country.get('region', 'N/A'),
            'Subregion': country.get('subregion', 'N/A'),
            'Population': country.get('population', 0),  # Ensure numerical consistency
            'Area': country.get('area', 0),
            'Languages': ', '.join(country.get('languages', {}).values()) if 'languages' in country else 'N/A',
            'Currencies': ', '.join([currency['name'] for currency in country.get('currencies', {}).values()]) if 'currencies' in country else 'N/A',
            'Flag': country.get('flags', {}).get('png', 'N/A')
        }
        countries.append(country_info)
    
    df = pd.DataFrame(countries)
    return df

# Function to upload data to PostgreSQL database
def upload_data_to_postgres(df):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    
    # Creating table with UNIQUE constraint on the name column
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS countries (
        name TEXT UNIQUE,
        capital TEXT,
        region TEXT,
        subregion TEXT,
        population BIGINT,
        area REAL,
        languages TEXT,
        currencies TEXT,
        flag TEXT
    );
    '''
    cur.execute(create_table_query)
    
    # Inserting data into the table
    for _, row in df.iterrows():
        insert_query = '''
        INSERT INTO countries (name, capital, region, subregion, population, area, languages, currencies, flag)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (name) DO UPDATE 
        SET capital = EXCLUDED.capital,
            region = EXCLUDED.region,
            subregion = EXCLUDED.subregion,
            population = EXCLUDED.population,
            area = EXCLUDED.area,
            languages = EXCLUDED.languages,
            currencies = EXCLUDED.currencies,
            flag = EXCLUDED.flag;
        '''
        cur.execute(insert_query, tuple(row))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    data = fetch_all_countries()
    df = transform_data(data)
    upload_data_to_postgres(df)
    print("Data has been successfully uploaded to the PostgreSQL database.")
