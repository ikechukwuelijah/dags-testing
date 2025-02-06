from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests

# Default DAG arguments
default_args = {
    'owner': 'Fm',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='weather_check_manchester',
    default_args=default_args,
    schedule_interval='@hourly',  # Runs every hour
    start_date=datetime(2025, 2, 6),
    catchup=False,
    tags=['example', 'weather']
) as dag:

    # Python function to fetch date, time, and temperature
    def check_weather():
        # Get the current date and time
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Get API key from Airflow variable
        api_key = Variable.get("openweather_api_key")

        # Coordinates for Manchester, UK
        lat = 53.483959
        lon = -2.244644

        # API request to One Call API 3.0
        response = requests.get(
            f'https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&units=metric&exclude=minutely,hourly,daily,alerts&appid={api_key}'
        )

        # Handle API response
        if response.status_code == 200:
            weather_data = response.json()
            temperature = weather_data['current']['temp']
            print(f"[{current_time}] Temperature in Manchester: {temperature}°C")
        else:
            print(f"[{current_time}] Failed to fetch weather data. Status code: {response.status_code}")

    # Task to call the Python function
    fetch_weather_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=check_weather,
    )

    fetch_weather_task
