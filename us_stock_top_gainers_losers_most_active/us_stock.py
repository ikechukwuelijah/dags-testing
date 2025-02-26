import requests
import pandas as pd

import requests
import pandas as pd
import psycopg2

# Alpha Vantage API Key
API_KEY = "D71T6RVK8CNYB0LS"

# PostgreSQL connection details
DB_PARAMS = {
    "dbname": "dwh",
    "user": "ikeengr",
    "password": "DataEngineer247",
    "host": "89.40.0.150",
    "port": "5432"
}

# Fetch stock market data
url = f"https://www.alphavantage.co/query?function=TOP_GAINERS_LOSERS&apikey=D71T6RVK8CNYB0LS"
r = requests.get(url)
data = r.json()

# Convert API response to DataFrames
top_gainers_df = pd.DataFrame(data.get("top_gainers", []))
top_losers_df = pd.DataFrame(data.get("top_losers", []))
most_actively_traded_df = pd.DataFrame(data.get("most_actively_traded", []))

# Combine all data into one DataFrame with category column
top_gainers_df["category"] = "top_gainer"
top_losers_df["category"] = "top_loser"
most_actively_traded_df["category"] = "most_active"

final_df = pd.concat([top_gainers_df, top_losers_df, most_actively_traded_df])

# Standardize column names
final_df.rename(columns={"ticker": "symbol", "price": "current_price", "change_percentage": "change_pct"}, inplace=True)

# Define PostgreSQL table name
TABLE_NAME = "us_stock_top_gainers_losers_most_active"

# Upload data to PostgreSQL
def upload_to_postgres(df, db_params, table_name):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Create table if not exists
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol TEXT PRIMARY KEY,
            current_price FLOAT,
            change_pct TEXT,
            volume BIGINT,
            category TEXT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Insert data
        for _, row in df.iterrows():
            try:
                cursor.execute(
                    f"""
                    INSERT INTO {table_name} (symbol, current_price, change_pct, volume, category)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (symbol) DO UPDATE 
                    SET current_price = EXCLUDED.current_price,
                        change_pct = EXCLUDED.change_pct,
                        volume = EXCLUDED.volume,
                        category = EXCLUDED.category;
                    """,
                    (row["symbol"], row["current_price"], row["change_pct"], row["volume"], row["category"])
                )
            except Exception as e:
                print(f"Error inserting {row['symbol']}: {e}")
                conn.rollback()
            else:
                conn.commit()
        
        # Close connections
        cursor.close()
        conn.close()
        print(f"Data successfully uploaded to {table_name}")
    
    except Exception as e:
        print(f"Database error: {e}")

# Call function to upload data
upload_to_postgres(final_df, DB_PARAMS, TABLE_NAME)

