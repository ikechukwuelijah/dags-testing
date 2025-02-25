import psycopg2
import requests
import pandas as pd

# PostgreSQL connection details
DB_NAME = "dwh"
DB_USER = "ikeengr"
DB_PASSWORD = "DataEngineer247"
DB_HOST = "89.40.0.150"  # Change if hosted remotely
DB_PORT = "5432"

# NewsAPI details
API_KEY = "9d64ba92867247f2a6c57a04a7eebc78"
URL = "https://newsapi.org/v2/top-headlines"
PARAMS = {
    "country": "us",
    "category": "technology",
    "apiKey": API_KEY
}

# Fetch news data from API
response = requests.get(URL, params=PARAMS)

if response.status_code == 200:
    news_data = response.json()
    articles = news_data.get("articles", [])

    if articles:
        # Convert to DataFrame
        df = pd.DataFrame(articles, columns=["title", "source", "url"])
        df["source"] = df["source"].apply(lambda x: x["name"] if isinstance(x, dict) else x)

        # Connect to PostgreSQL
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            cursor = conn.cursor()

            # Insert data into PostgreSQL
            for _, row in df.iterrows():
                try:
                    cursor.execute(
                        """
                        INSERT INTO tech_news (title, source, url)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (url) DO NOTHING;
                        """,
                        (row["title"], row["source"], row["url"])
                    )
                except Exception as e:
                    print(f"Error inserting row: {e}")
                    conn.rollback()  # Rollback the transaction on failure
                else:
                    conn.commit()  # Commit only when successful

            print("Data inserted successfully!")

        except Exception as e:
            print("Database connection error:", e)

        finally:
            cursor.close()
            conn.close()

    else:
        print("No news articles found.")

else:
    print("Error fetching data:", response.status_code, response.text)
