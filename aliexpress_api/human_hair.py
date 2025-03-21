#%% Import necessary libraries
import requests
import pandas as pd
import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

#%% Step 1: Extract Data from API
url = "https://aliexpress-business-api.p.rapidapi.com/textsearch.php"

querystring = {"keyWord": "human hair", "country": "NG", "currency": "USD", "lang": "en", "filter": "orders", "sortBy": "asc"}

headers = {
    "x-rapidapi-key": "efbc12a764msh39a81e663d3e104p1e76acjsn337fd1d56751",
    "x-rapidapi-host": "aliexpress-business-api.p.rapidapi.com"
}

try:
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()  # Raise an error if the request fails
    data = response.json()
    logging.info("API data fetched successfully.")
except Exception as e:
    logging.error(f"Failed to fetch API data: {e}")
    data = {}

#%% Step 2: Transform Data
if "data" in data and "itemList" in data["data"]:
    items = data["data"]["itemList"]
    df = pd.DataFrame(items)

    # Renaming columns to match PostgreSQL table schema
    column_mapping = {
        "itemId": "item_id",
        "title": "title",
        "originalPrice": "original_price",
        "salePrice": "sale_price",
        "discount": "discount",
        "itemMainPic": "item_main_pic",
        "type": "type",
        "score": "score",
        "cateId": "cate_id",
        "targetSalePrice": "target_sale_price",
        "targetOriginalPrice": "target_original_price",
        "salePriceCurrency": "sale_price_currency",
        "originalPriceCurrency": "original_price_currency",
        "orders": "orders",
        "originMinPrice": "origin_min_price",
        "evaluateRate": "evaluate_rate",
        "salePriceFormat": "sale_price_format",
        "targetOriginalPriceCurrency": "target_original_price_currency"
    }

    df.rename(columns=column_mapping, inplace=True)
    df.fillna("", inplace=True)  # Replace NaN with empty strings or 0
    logging.info(f"Transformed data: {df.shape[0]} records ready for loading.")
else:
    logging.warning("No items found in the API response.")
    df = pd.DataFrame()  # Empty DataFrame

#%% Step 3: Load Data into PostgreSQL
if not df.empty:
    db_config = {
        "dbname": "dwh",
        "user": "ikeengr",
        "password": "DataEngineer247",
        "host": "89.40.0.150",
        "port": "5432"
    }

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        logging.info("Connected to the database.")

        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS aliexpress_human_hair (
            item_id VARCHAR(50) PRIMARY KEY,
            title TEXT,
            original_price NUMERIC,
            sale_price NUMERIC,
            discount TEXT,
            item_main_pic TEXT,
            type TEXT,
            score TEXT,
            cate_id TEXT,
            target_sale_price NUMERIC,
            target_original_price NUMERIC,
            sale_price_currency TEXT,
            original_price_currency TEXT,
            orders TEXT,
            origin_min_price TEXT,
            evaluate_rate TEXT,
            sale_price_format TEXT,
            target_original_price_currency TEXT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        logging.info("Table checked/created successfully.")

        # Insert data into the database using batch insert
        insert_query = """
        INSERT INTO aliexpress_human_hair (
            item_id, title, original_price, sale_price, discount, item_main_pic,
            type, score, cate_id, target_sale_price, target_original_price,
            sale_price_currency, original_price_currency, orders, origin_min_price,
            evaluate_rate, sale_price_format, target_original_price_currency
        )
        VALUES %s
        ON CONFLICT (item_id) DO NOTHING;
        """

        from psycopg2.extras import execute_values
        records = [
            (
                row.item_id, row.title, float(row.original_price or 0.0), float(row.sale_price or 0.0),
                row.discount, row.item_main_pic, row.type, row.score, row.cate_id,
                float(row.target_sale_price or 0.0), float(row.target_original_price or 0.0),
                row.sale_price_currency, row.original_price_currency, row.orders,
                row.origin_min_price, row.evaluate_rate, row.sale_price_format,
                row.target_original_price_currency
            )
            for _, row in df.iterrows()
        ]

        execute_values(cursor, insert_query, records)
        conn.commit()
        logging.info(f"Successfully inserted {len(records)} records into the database.")

    except Exception as e:
        logging.error(f"An error occurred during database operation: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            logging.info("Database connection closed.")
else:
    logging.warning("No data to load into the database.")

# %%
