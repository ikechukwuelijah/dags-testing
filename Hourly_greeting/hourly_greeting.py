# Import necessary modules from Airflow and Python's standard library
from airflow import DAG  # Import the DAG class to define Directed Acyclic Graphs
from airflow.operators.python import PythonOperator  # Import the PythonOperator to execute Python functions as tasks
from datetime import datetime, timedelta  # Import datetime and timedelta for date and time operations

# Define a function that determines the greeting based on the current time of day
def greet_and_print_datetime():
    """
    This function retrieves the current date, time, and month, and prints a greeting message
    based on the time of day (morning, afternoon, or evening).
    """
    now = datetime.now()  # Get the current date and time
    current_hour = now.hour  # Extract the current hour (0-23 format)
    current_date = now.strftime("%Y-%m-%d")  # Format the date as "YYYY-MM-DD"
    current_time = now.strftime("%H:%M:%S")  # Format the time as "HH:MM:SS"
    current_month = now.strftime("%B")  # Get the full name of the current month (e.g., "October")

    # Determine the appropriate greeting based on the current hour
    if 5 <= current_hour < 12:  # If the hour is between 5 AM and 12 PM
        greeting = "Good Morning!"
    elif 12 <= current_hour < 18:  # If the hour is between 12 PM and 6 PM
        greeting = "Good Afternoon!"
    else:  # Otherwise (between 6 PM and 5 AM the next day)
        greeting = "Good Evening!"

    # Print the current date, time, month, and the determined greeting
    print(f"Date: {current_date}")  # Print the formatted date
    print(f"Time: {current_time}")  # Print the formatted time
    print(f"Month: {current_month}")  # Print the current month
    print(greeting)  # Print the greeting message

# Define default arguments for the DAG
default_args = {
    'owner': 'Fm',  # The owner of this DAG
    'depends_on_past': False,  # Whether this DAG depends on the success of previous runs
    'email_on_failure': False,  # Whether to send an email on task failure
    'email_on_retry': False,  # Whether to send an email on task retry
    'retries': 1,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5),  # Time delay between retries (in this case, 5 minutes)
}

# Create the DAG object
with DAG(
    'greet_and_print_datetime_dag',  # The unique identifier for this DAG
    default_args=default_args,  # Use the default arguments defined above
    description='A DAG that prints the date, time, month, and a greeting message',  # Description of the DAG
    schedule_interval='@hourly',  # Schedule the DAG to run every hour
    start_date=datetime(2023, 10, 1),  # Set the start date for the DAG (it will not run before this date)
    catchup=False  # Do not backfill past runs if the DAG is started late
) as dag:

    # Define a task using the PythonOperator
    greet_task = PythonOperator(
        task_id='greet_and_print_datetime',  # Unique identifier for this task
        python_callable=greet_and_print_datetime,  # The Python function to execute
    )

# Task dependencies (only one task in this case)
# The greet_task will be executed whenever the DAG runs
greet_task