#%%
import requests
import pandas as pd
import psycopg2

# ----------------------------
# Step 1: Extract - API Request
# ----------------------------


url = "https://game-quiz.p.rapidapi.com/quiz/random"

querystring = {"lang":"en","amount":"100","format":"mcq","cached":"false","endpoint":"games"}

headers = {
	"x-rapidapi-key": "e7cbee4ae1mshd0704b6dcc65548p1632e3jsncb04d537a049",
	"x-rapidapi-host": "game-quiz.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())

#%% Convert JSON response to DataFrame
# ----------------------------
# Step 2: Transform - JSON to DataFrame
# ----------------------------

json_data = response.json()
records = []

for item in json_data.get("data", []):
    question = item.get("question")
    correct = item.get("options", {}).get("correct")
    incorrect = item.get("options", {}).get("incorrect", [])

    # Pad or truncate incorrect answers to always have 3
    incorrect = (incorrect + [None] * 3)[:3]

    record = {
        "question": question,
        "correct_answer": correct,
        "option_1": correct,
        "option_2": incorrect[0],
        "option_3": incorrect[1],
        "option_4": incorrect[2],
        "reference": item.get("reference", [None])[0],
        "extra_type": item.get("extra", {}).get("type"),
        "extra_content": item.get("extra", {}).get("content"),
        "is_image": item.get("options", {}).get("is_image", False)
    }
    records.append(record)

df = pd.DataFrame(records)
print(df.head())


# ----------------------------
# Step 3: Load - To PostgreSQL
# ----------------------------

#%% PostgreSQL connection settings
import psycopg2
from psycopg2 import sql

# ----------------------------
# Step 3: Load Data to PostgreSQL
# ----------------------------

# Database connection parameters
db_params = {
    "dbname": "dwh",
    "user": "ikeengr",
    "password": "DataEngineer247",
    "host": "89.40.0.150",
    "port": "5432"
}

# Create a connection to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Create table if it does not exist
create_table_query = """
CREATE TABLE IF NOT EXISTS random_mcq_game_quiz (
    id SERIAL PRIMARY KEY,
    question TEXT,
    correct_answer TEXT,
    option_1 TEXT,
    option_2 TEXT,
    option_3 TEXT,
    option_4 TEXT,
    reference TEXT,
    extra_type TEXT,
    extra_content TEXT,
    is_image BOOLEAN
);
"""

cur.execute(create_table_query)
conn.commit()

# Insert DataFrame into PostgreSQL
for index, row in df.iterrows():
    insert_query = """
    INSERT INTO random_mcq_game_quiz (question, correct_answer, option_1, option_2, option_3, option_4, reference, extra_type, extra_content, is_image)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (row['question'], row['correct_answer'], row['option_1'], row['option_2'], row['option_3'], row['option_4'], row['reference'], row['extra_type'], row['extra_content'], row['is_image'])
    
    cur.execute(insert_query, values)

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()

print("Data loaded successfully into PostgreSQL!")



# %%
