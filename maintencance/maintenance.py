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
RETENTION_DAYS = int(Variable.get("retention_days", default_var=7))

# Cleanup Logs
def cleanup_logs():
    for root, dirs, files in os.walk(AIRFLOW_LOGS_DIR, topdown=False):
        for file in files:
            file_path = os.path.join(root, file)
            if os.path.isfile(file_path) and (days_ago(RETENTION_DAYS).date() > os.path.getmtime(file_path)):
                os.remove(file_path)
                logging.info(f"Deleted log file: {file_path}")
        for dir in dirs:
            dir_path = os.path.join(root, dir)
            if not os.listdir(dir_path):
                shutil.rmtree(dir_path)
                logging.info(f"Deleted empty directory: {dir_path}")

# Cleanup XCom
@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).filter(XCom.execution_date < days_ago(RETENTION_DAYS)).delete(synchronize_session=False)
    session.commit()
    logging.info("Deleted old XCom entries.")

# Vacuum & Analyze for PostgreSQL
def vacuum_analyze():
    postgres_engine = create_engine(POSTGRES_CONN_URI)
    with postgres_engine.connect() as connection:
        connection.execute(text("VACUUM ANALYZE;"))
        logging.info("Vacuum & Analyze completed on PostgreSQL.")

# Define DAG
def create_maintenance_dag(dag_id, python_callable, schedule_interval):
    return DAG(
        dag_id=dag_id,
        default_args={
            "owner": "airflow",
            "start_date": days_ago(1),
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        schedule_interval=schedule_interval,
        catchup=False,
        tags=["maintenance"],
    )

# DAGs
log_cleanup_dag = create_maintenance_dag("cleanup_logs", cleanup_logs, "@weekly")
xcom_cleanup_dag = create_maintenance_dag("cleanup_xcom", cleanup_xcom, "@weekly")
vacuum_analyze_dag = create_maintenance_dag("vacuum_analyze", vacuum_analyze, "@weekly")

# Tasks
with log_cleanup_dag:
    PythonOperator(
        task_id="cleanup_logs",
        python_callable=cleanup_logs,
    )

with xcom_cleanup_dag:
    PythonOperator(
        task_id="cleanup_xcom",
        python_callable=cleanup_xcom,
    )

with vacuum_analyze_dag:
    PythonOperator(
        task_id="vacuum_analyze",
        python_callable=vacuum_analyze,
    )
