from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

import requests
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import smtplib
from email.message import EmailMessage

# Configs
API_KEY = "fc284336c861c52e8185c63082114ad5"
LAT = 51.5074
LON = -0.1278
CITY_NAME = "London"
LOCATION_NAME = "London Central AQ"
DB_CONFIG = {
    "dbname": "dwh",
    "user": "ikeengr",
    "password": "DataEngineer247",
    "host": "89.40.0.150",
    "port": "5432"
}
EXPECTED_MIN_RECORDS = 10
EMAIL_SENDER = "youremail@gmail.com"
EMAIL_PASSWORD = "your_app_password"
EMAIL_RECEIVERS = ["recipient1@example.com", "recipient2@example.com"]

# Store in XCom-safe location
EXTRACT_PATH = "/tmp/raw_air_quality.csv"

@dag(schedule="0 6 * * *", start_date=days_ago(1), catchup=False, tags=["air_quality"])
def city_metrics_pipeline():

    @task
    def extract_data():
        now = datetime.utcnow()
        yesterday = now - timedelta(days=1)
        start_ts = int(yesterday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        end_ts = int(yesterday.replace(hour=23, minute=59, second=59, microsecond=0).timestamp())

        url = "http://api.openweathermap.org/data/2.5/air_pollution/history"
        params = {
            "lat": LAT,
            "lon": LON,
            "start": start_ts,
            "end": end_ts,
            "appid": API_KEY
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        raw_data = response.json().get("list", [])

        rows = []
        for entry in raw_data:
            row = {
                "timestamp": datetime.utcfromtimestamp(entry["dt"]),
                "city": CITY_NAME,
                "location": LOCATION_NAME,
                "aqi": entry["main"]["aqi"],
                **entry["components"]
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        df.rename(columns={"pm2_5": "pm25"}, inplace=True)
        df.to_csv(EXTRACT_PATH, index=False)

    @task
    def load_raw():
        df = pd.read_csv(EXTRACT_PATH, parse_dates=["timestamp"])
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_air_quality (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            city TEXT,
            location TEXT,
            aqi INTEGER,
            co FLOAT, no FLOAT, no2 FLOAT,
            o3 FLOAT, so2 FLOAT,
            pm25 FLOAT, pm10 FLOAT, nh3 FLOAT
        )
        """)
        conn.commit()

        insert_raw = """
        INSERT INTO raw_air_quality
        (timestamp, city, location, aqi, co, no, no2, o3, so2, pm25, pm10, nh3)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            cur.execute(insert_raw, (
                row.timestamp, row.city, row.location, row.aqi,
                row.get("co"), row.get("no"), row.get("no2"),
                row.get("o3"), row.get("so2"),
                row.get("pm25"), row.get("pm10"), row.get("nh3")
            ))
        conn.commit()
        cur.close()
        conn.close()

    @task
    def clean_data():
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS stg_air_quality (
            city TEXT,
            location TEXT,
            parameter TEXT,
            clean_value NUMERIC(10,2),
            unit TEXT,
            measurement_date DATE
        )
        """)
        conn.commit()

        cur.execute("""
        INSERT INTO stg_air_quality (city, location, parameter, clean_value, unit, measurement_date)
        SELECT city, location, 'pm25', ROUND(pm25::NUMERIC,2), 'µg/m³', timestamp::DATE
        FROM raw_air_quality WHERE pm25 IS NOT NULL
        UNION ALL
        SELECT city, location, 'pm10', ROUND(pm10::NUMERIC,2), 'µg/m³', timestamp::DATE
        FROM raw_air_quality WHERE pm10 IS NOT NULL
        """)
        conn.commit()
        cur.close()
        conn.close()

    create_dim_and_fact = PostgresOperator(
        task_id="load_staging",
        postgres_conn_id="your_postgres_conn",  # Define in Airflow Connections UI
        sql="""
        CREATE TABLE IF NOT EXISTS dim_location (
            location_id SERIAL PRIMARY KEY,
            city TEXT,
            location TEXT
        );

        CREATE TABLE IF NOT EXISTS fact_air_quality (
            location_id INT REFERENCES dim_location(location_id),
            parameter TEXT,
            measurement_date DATE,
            avg_value DOUBLE PRECISION
        );

        INSERT INTO dim_location (city, location)
        SELECT DISTINCT city, location FROM stg_air_quality
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_location d
            WHERE d.city = stg_air_quality.city AND d.location = stg_air_quality.location
        );

        INSERT INTO fact_air_quality (location_id, parameter, measurement_date, avg_value)
        SELECT d.location_id, s.parameter, s.measurement_date, AVG(s.clean_value)
        FROM stg_air_quality s
        JOIN dim_location d ON s.city = d.city AND s.location = d.location
        GROUP BY d.location_id, s.parameter, s.measurement_date;
        """
    )

    @task
    def validate_data():
        def send_email_alert(subject, body):
            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = EMAIL_SENDER
            msg["To"] = ", ".join(EMAIL_RECEIVERS)
            msg.set_content(body)

            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(EMAIL_SENDER, EMAIL_PASSWORD)
                server.send_message(msg)

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fact_air_quality")
        row_count = cur.fetchone()[0]

        if row_count < EXPECTED_MIN_RECORDS:
            subject = "🚨 Air Quality ETL Alert: Low Data Volume"
            body = f"""
            Alert from ETL pipeline:

            Table: fact_air_quality
            Row count: {row_count}
            Expected: {EXPECTED_MIN_RECORDS}

            Timestamp: {datetime.utcnow().isoformat()} UTC
            """
            send_email_alert(subject, body)

        cur.close()
        conn.close()

    # Task orchestration
    extracted = extract_data()
    raw_loaded = load_raw()
    cleaned = clean_data()
    validated = validate_data()

    extracted >> raw_loaded >> cleaned >> create_dim_and_fact >> validated

dag_instance = city_metrics_pipeline()
