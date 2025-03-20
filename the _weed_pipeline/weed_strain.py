#%%
import requests
import pandas as pd
import psycopg2

url = "https://weed-strain1.p.rapidapi.com/"

querystring = {"ordering":"strain"}

headers = {
	"x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
	"x-rapidapi-host": "weed-strain1.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())
# %% # Transform API output to Pandas DataFrame
data = response.json()
df = pd.DataFrame(data)
print(df.head())

# %%
# Load DataFrame into PostgreSQL
def load_data_to_postgres(df):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="dwh",
            user="ikeengr",
            password="DataEngineer247",
            host="89.40.0.150",
            port="5432"
        )
        cursor = conn.cursor()
        
        # Create table if not exists
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS strains (
            id SERIAL PRIMARY KEY,
            strain TEXT,
            thc TEXT,
            cbd TEXT,
            cbg TEXT,
            strain_type TEXT,
            climate TEXT,
            difficulty TEXT,
            fungal_resistance TEXT,
            indoor_yield_max FLOAT,
            outdoor_yield_max FLOAT,
            flowering_weeks_min FLOAT,
            flowering_weeks_max FLOAT,
            height_inches_min FLOAT,
            height_inches_max FLOAT,
            good_effects TEXT,
            side_effects TEXT,
            img_thumb TEXT,
            img_attribution TEXT,
            img_attribution_link TEXT,
            img_creative_commons BOOLEAN
        );
        '''
        cursor.execute(create_table_query)
        conn.commit()
        
        # Insert data into the table
        for _, row in df.iterrows():
            cursor.execute('''
                INSERT INTO strains (id, strain, thc, cbd, cbg, strain_type, climate, difficulty, fungal_resistance,
                                    indoor_yield_max, outdoor_yield_max, flowering_weeks_min, flowering_weeks_max,
                                    height_inches_min, height_inches_max, good_effects, side_effects, img_thumb,
                                    img_attribution, img_attribution_link, img_creative_commons)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                row['id'], row['strain'], row['thc'], row['cbd'], row['cbg'], row['strainType'], row['climate'],
                row['difficulty'], row['fungalResistance'], row['indoorYieldInGramsMax'], row['outdoorYieldInGramsMax'],
                row['floweringWeeksMin'], row['floweringWeeksMax'], row['heightInInchesMin'], row['heightInInchesMax'],
                row['goodEffects'], row['sideEffects'], row['imgThumb'], row['imgAttribution'],
                row['imgAttributionLink'], row['imgCreativeCommons']
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully loaded into PostgreSQL.")
    except Exception as e:
        print(f"Error: {e}")

# Call function to load data into PostgreSQL
load_data_to_postgres(df)

# %%
