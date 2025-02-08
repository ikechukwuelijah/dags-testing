# Import necessary libraries
from airflow import DAG  # Import the DAG class to define the Directed Acyclic Graph
from airflow.operators.python_operator import PythonOperator  # Import PythonOperator to execute Python functions
from datetime import datetime  # Import datetime for scheduling and time-related operations
import pytz  # Import pytz for timezone handling

# Define a function to check the current hour and print the appropriate message
def print_greeting():
    """
    This function checks the current hour and prints a greeting message if the hour matches
    one of the predefined times (12am, 3am, 6am, 9am, 12pm, 3pm, 6pm, 9pm).
    Otherwise, it prints 'No greetings'.
    """
    # Get the current time in UTC (or set your desired timezone)
    utc_now = datetime.now(pytz.utc)  # Use pytz.utc for UTC timezone
    current_hour = utc_now.hour  # Extract the current hour (0-23 in 24-hour format)

    # Define the list of hours when greetings should be printed (in 24-hour format)
    greeting_hours = [0, 3, 6, 9, 12, 15, 18, 21]  # 12am, 3am, 6am, 9am, 12pm, 3pm, 6pm, 9pm

    # Check if the current hour is in the list of greeting hours
    if current_hour in greeting_hours:
        print(f"Greetings! The current hour is {current_hour}:00")  # Print greeting message
    else:
        print("No greetings")  # Print 'No greetings' for other hours

# Define default arguments for the DAG
default_args = {
    'owner': 'Ik',  # Owner of the DAG
    'start_date': datetime(2023, 1, 1),  # Start date for the DAG (set to January 1, 2023)
    'retries': 1,  # Number of retries in case of failure
}

# Define the DAG
with DAG(
    'hourly_greeting_dag',  # Name of the DAG
    default_args=default_args,  # Default arguments for the DAG
    description='A DAG that prints greetings at specific hours',  # Description of the DAG
    schedule_interval='0 * * * *',  # Schedule interval: Runs every hour at minute 0 (e.g., 12:00, 1:00, etc.)
    catchup=False,  # Disable backfilling (only run for current and future intervals)
) as dag:

    # Define the task using PythonOperator
    print_greeting_task = PythonOperator(
        task_id='print_greeting_task',  # Unique identifier for the task
        python_callable=print_greeting,  # The Python function to execute
    )

    # Set the task to run
    print_greeting_task