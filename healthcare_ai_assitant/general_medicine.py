#%%
import requests
import pandas as pd
import psycopg2

url = "https://ai-doctor-api-ai-medical-chatbot-healthcare-ai-assistant.p.rapidapi.com/chat"

querystring = {"noqueue":"1"}

payload = {
	"message": "What are the common symptoms of flu?",
	"specialization": "general",
	"language": "en"
}
headers = {
	"x-rapidapi-key": "efbc12a764msh39a81e663d3e104p1e76acjsn337fd1d56751",
	"x-rapidapi-host": "ai-doctor-api-ai-medical-chatbot-healthcare-ai-assistant.p.rapidapi.com",
	"Content-Type": "application/json"
}

response = requests.post(url, json=payload, headers=headers, params=querystring)

print(response.json())

#%% Convert JSON to DataFrame
# Parse the JSON response
data = response.json()

# Flatten the nested JSON structure
flattened_data = {
    "message": data.get("result", {}).get("response", {}).get("message"),
    "recommendations": ", ".join(data.get("result", {}).get("response", {}).get("recommendations", [])),
    "warnings": ", ".join(data.get("result", {}).get("response", {}).get("warnings", [])),
    "references": ", ".join(data.get("result", {}).get("response", {}).get("references", [])),
    "followUp": ", ".join(data.get("result", {}).get("response", {}).get("followUp", [])),
    "specialization": data.get("result", {}).get("metadata", {}).get("specialization"),
    "confidence": data.get("result", {}).get("metadata", {}).get("confidence"),
    "requiresPhysicianConsult": data.get("result", {}).get("metadata", {}).get("requiresPhysicianConsult"),
    "emergencyLevel": data.get("result", {}).get("metadata", {}).get("emergencyLevel"),
    "topRelatedSpecialties": ", ".join(data.get("result", {}).get("metadata", {}).get("topRelatedSpecialties", [])),
    "cacheTime": data.get("cacheTime"),
    "status": data.get("status"),
    "apiMessage": data.get("message"),
    "language": data.get("metadata", {}).get("language"),
    "queryTime": data.get("metadata", {}).get("queryTime"),
    "time": data.get("time")
}

# Convert flattened data into a DataFrame
df = pd.DataFrame([flattened_data])

# Check the number of columns in the DataFrame
num_columns = len(df.columns)

# Print the DataFrame and the number of columns
print("DataFrame:")
print(df)
print("\nNumber of Columns:", num_columns)


# %%

# Database credentials
dbname = "dwh"
user = "ikeengr"
password = "DataEngineer247"
host = "89.40.0.150"
port = "5432"

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cursor = conn.cursor()

    # Define the INSERT query
    insert_query = """
    INSERT INTO general_medicine (
        message, recommendations, warnings, references, follow_up, specialization,
        confidence, requires_physician_consult, emergency_level, top_related_specialties,
        cache_time, status, api_message, language, query_time, time
    ) VALUES (
        %(message)s, %(recommendations)s, %(warnings)s, %(references)s, %(follow_up)s,
        %(specialization)s, %(confidence)s, %(requires_physician_consult)s, %(emergency_level)s,
        %(top_related_specialties)s, %(cache_time)s, %(status)s, %(api_message)s, %(language)s,
        %(query_time)s, %(time)s
    );
    """

    # Execute the INSERT query with the record dictionary
    cursor.execute(insert_query, record)
    conn.commit()

    print("Data successfully inserted into the 'general_medicine' table.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    if conn:
        cursor.close()
        conn.close()