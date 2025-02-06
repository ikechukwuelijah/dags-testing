from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments to be passed to tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    'sample_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=['test'],
) as dag:

    # Task to print a message
    task_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello from Airflow!"'
    )

    # Task to check the current date
    task_date = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    # Define task dependencies
    task_hello >> task_date
