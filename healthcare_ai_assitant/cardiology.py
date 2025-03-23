import requests
import pandas as pd
import psycopg2

# API request
url = "https://ai-doctor-api-ai-medical-chatbot-healthcare-ai-assistant.p.rapidapi.com/chat"
querystring = {"noqueue": "1"}

payload = {
    "message": "What are heart attack symptoms?",
    "specialization": "cardiology",
    "language": "en"
}
headers = {
    "x-rapidapi-key": "efbc12a764msh39a81e663d3e104p1e76acjsn337fd1d56751",
    "x-rapidapi-host": "ai-doctor-api-ai-medical-chatbot-healthcare-ai-assistant.p.rapidapi.com",
    "Content-Type": "application/json"
}

response = requests.post(url, json=payload, headers=headers, params=querystring)
data = response.json()

# Extract relevant data
response_data = data.get("result", {}).get("response", {})
metadata = data.get("metadata", {})

# Convert data to DataFrame
df = pd.DataFrame({
    "message": [response_data.get("message", "")],
    "recommendations": [" | ".join(response_data.get("recommendations", []))],
    "warnings": [" | ".join(response_data.get("warnings", []))],
    "reference_links": [" | ".join(response_data.get("references", []))],  # Renamed
    "followUp": [" | ".join(response_data.get("followUp", []))],
    "specialization": [metadata.get("specialization", "")],
    "confidence": [data.get("result", {}).get("metadata", {}).get("confidence", "")],
    "requiresPhysicianConsult": [data.get("result", {}).get("metadata", {}).get("requiresPhysicianConsult", "")],
    "emergencyLevel": [data.get("result", {}).get("metadata", {}).get("emergencyLevel", "")],
})

# Load data to PostgreSQL
def load_to_postgres(df):
    try:
        conn = psycopg2.connect(
            dbname="dwh",
            user="ikeengr",
            password="DataEngineer247",
            host="89.40.0.150",
            port="5432"
        )
        cur = conn.cursor()
        
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS heart_attack_info (
            id SERIAL PRIMARY KEY,
            message TEXT,
            recommendations TEXT,
            warnings TEXT,
            reference_links TEXT,  -- Renamed here
            followUp TEXT,
            specialization TEXT,
            confidence TEXT,
            requiresPhysicianConsult BOOLEAN,
            emergencyLevel TEXT
        );
        '''
        cur.execute(create_table_query)
        
        insert_query = '''
        INSERT INTO heart_attack_info (
            message, recommendations, warnings, reference_links, followUp, specialization, confidence, requiresPhysicianConsult, emergencyLevel
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        '''
        
        for _, row in df.iterrows():
            cur.execute(insert_query, tuple(row))
        
        conn.commit()
        cur.close()
        conn.close()
        print("Data successfully inserted into PostgreSQL.")
    except Exception as e:
        print("Error inserting data into PostgreSQL:", e)

# Call function to load data
load_to_postgres(df)
