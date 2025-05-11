# Single Airflow DAG for all maintenance tasks (Logs/XCom/Vacuum)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import shutil
import logging
from datetime import timedelta
from airflow.models import XCom, Variable
from airflow.utils.db import provide_session
from sqlalchemy import create_engine, text

# Retrieve Airflow Variables
AIRFLOW_LOGS_DIR = Variable.get("airflow_logs_dir")
POSTGRES_CONN_URI = Variable.get("postgres_conn_uri")
RETENTION_DAYS = int(Variable.get("retention_days", default_var=6))

# Cleanup Logs

def cleanup_logs():
    cutoff_ts = days_ago(RETENTION_DAYS).timestamp()
    deleted_files_count = 0
    deleted_dirs_count = 0

    for root, dirs, files in os.walk(AIRFLOW_LOGS_DIR, topdown=False):
        for file in files:
            file_path = os.path.join(root, file)
            # Compare modification time
            if os.path.isfile(file_path) and os.path.getmtime(file_path) < cutoff_ts:
                os.remove(file_path)
                deleted_files_count += 1
        # Remove empty directories after deleting files
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            if not os.listdir(dir_path):
                shutil.rmtree(dir_path)
                deleted_dirs_count += 1

    logging.info(f"Deleted {deleted_files_count} old log files and {deleted_dirs_count} empty directories.")

# Cleanup XCom
@provide_session
def cleanup_xcom(session=None):
    # Query old XCom records older than RETENTION_DAYS
    older_xcom_query = session.query(XCom).filter(XCom.execution_date < days_ago(RETENTION_DAYS))
    count_deleted = older_xcom_query.count()

    # Delete them
    older_xcom_query.delete(synchronize_session=False)
    session.commit()
    logging.info(f"Deleted {count_deleted} old XCom entries.")

# Vacuum & Analyze for PostgreSQL

def vacuum_analyze():
    # Use AUTOCOMMIT so VACUUM can run outside a transaction block
    postgres_engine = create_engine(POSTGRES_CONN_URI, isolation_level="AUTOCOMMIT")
    with postgres_engine.connect() as connection:
        connection.execute(text("VACUUM ANALYZE;"))
        logging.info("Vacuum & Analyze completed on PostgreSQL.")

# Default args for the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create a single DAG to handle all maintenance tasks
with DAG(
    dag_id="maintenance_dag",
    default_args=default_args,
    schedule_interval="@weekly",  # runs weekly
    catchup=False,
    tags=["maintenance"],
) as dag:

    # Task 1: Cleanup logs
    cleanup_logs_task = PythonOperator(
        task_id="cleanup_logs",
        python_callable=cleanup_logs,
    )

    # Task 2: Cleanup XCom
    cleanup_xcom_task = PythonOperator(
        task_id="cleanup_xcom",
        python_callable=cleanup_xcom,
    )

    # Task 3: Vacuum & Analyze
    vacuum_analyze_task = PythonOperator(
        task_id="vacuum_analyze",
        python_callable=vacuum_analyze,
    )

    # Optional: define task ordering if desired
    # cleanup_logs_task >> cleanup_xcom_task >> vacuum_analyze_task
