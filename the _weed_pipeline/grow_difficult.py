import pandas as pd
import psycopg2
import requests

# API request
url = "https://the-weed-db.p.rapidapi.com/api/strains"
querystring = {"growDifficulty": "difficult"}
headers = {
    "x-rapidapi-key": "f38eae887bmsh5211e33c97c1c50p125cafjsnec52eb060a05",
    "x-rapidapi-host": "the-weed-db.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

# Check for successful request
if response.status_code == 200:
    data = response.json()
    
    # Convert API response to DataFrame
    df = pd.DataFrame(data)

    # Debug: Show the column names and first few rows of the DataFrame
    print("Columns in DataFrame:", df.columns)
    print(df.head())

    # Database connection parameters
    db_params = {
        "dbname": "dwh",
        "user": "ikeengr",
        "password": "DataEngineer247",
        "host": "89.40.0.150",
        "port": "5432"
    }

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        print("Connected to PostgreSQL successfully.")

        # Create table (if it doesn't exist)
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS difficult_grow_strains (
            id TEXT PRIMARY KEY,
            name TEXT,
            link TEXT,
            imageUrl TEXT,
            description TEXT,
            genetics TEXT,
            THC TEXT,
            CBD TEXT,
            parents TEXT,
            smellAndFlavour TEXT,
            effect TEXT,
            growEnvironments TEXT,
            growDifficulty TEXT,
            floweringType TEXT,
            floweringTime TEXT,
            harvestTimeOutdoor TEXT,
            yieldIndoor TEXT,
            yieldOutdoor TEXT,
            heightIndoor TEXT,
            heightOutdoor TEXT,
            fromSeedToHarvest TEXT
        );
        '''
        cur.execute(create_table_query)  # Ensure table creation is executed
        conn.commit()

        # Insert data into the table
        insert_query = '''
        INSERT INTO difficult_grow_strains (id, name, link, imageUrl, description, genetics, THC, CBD, 
                                         parents, smellAndFlavour, effect, growEnvironments, growDifficulty, 
                                         floweringType, floweringTime, harvestTimeOutdoor, yieldIndoor, 
                                         yieldOutdoor, heightIndoor, heightOutdoor, fromSeedToHarvest)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;  -- Avoid duplicate inserts
        '''
        
        # Iterate over DataFrame rows and insert data
        for _, row in df.iterrows():
            values = (
                row.get('_id', None), 
                row.get('name', None),
                row.get('link', None),
                row.get('imageUrl', None),
                row.get('description', None),
                row.get('genetics', None),
                row.get('THC', None),
                row.get('CBD', None),
                row.get('parents', None),
                row.get('smellAndFlavour', None),
                row.get('effect', None),
                row.get('growEnvironments', None),
                row.get('growDifficulty', None),
                row.get('floweringType', None),
                row.get('floweringTime', None),
                row.get('harvestTimeOutdoor', None),
                row.get('yieldIndoor', None),
                row.get('yieldOutdoor', None),
                row.get('heightIndoor', None),
                row.get('heightOutdoor', None),
                row.get('fromSeedToHarvest', None)  # Some records might not have this
            )
            
            # Debug: Print the values being inserted
            print("Inserting values:", values)
            
            # Execute the query with values as a flat tuple
            cur.execute(insert_query, values)

        conn.commit()
        print("Data inserted successfully into PostgreSQL table.")

    except psycopg2.Error as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("PostgreSQL connection closed.")

else:
    print("Failed to fetch data. Status Code:", response.status_code)
