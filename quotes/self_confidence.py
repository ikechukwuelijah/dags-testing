from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import requests
import psycopg2
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

default_args = {
    'owner': 'Ikeengr',
    'start_date': datetime(2025, 4, 8),
    'retries': 1
}

dag = DAG(
    'self_confidence_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Task 1: Fetch quote from API
def fetch_quote(**kwargs):
    url = "https://quotes-api12.p.rapidapi.com/quotes/random"
    querystring = {"type": "selfconfidence"}
    headers = {
        "x-rapidapi-key": "efbc12a764msh39a81e663d3e104p1e76acjsn337fd1d56751",
        "x-rapidapi-host": "quotes-api12.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    quote_data = response.json()
    
    # Push to XCom
    kwargs['ti'].xcom_push(key='quote_data', value=quote_data)

# Task 2: Send plain text email
def send_quote_as_text_email(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_quote', key='quote_data')

    # Get email config from Airflow Variables
    email_config = Variable.get("email_config", deserialize_json=True)
    smtp_host = email_config.get('smtp_host')
    smtp_port = email_config.get('smtp_port')
    smtp_user = email_config.get('smtp_user')
    smtp_password = email_config.get('smtp_password')
    sender_email = email_config.get('sender_email')
    receiver_email = email_config.get('receiver_email')

    # Construct plain text email body
    quote = data.get('quote', 'No quote found.')
    author = data.get('author', 'Unknown')
    qtype = data.get('type', 'N/A')
    body = f"Here is your daily self-confidence quote:\n\n\"{quote}\"\n\n- {author} ({qtype})"

    # Create email message
    msg = MIMEMultipart()
    msg['Subject'] = 'Daily Self-Confidence Quote'
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)
    msg.attach(MIMEText(body, 'plain'))

    # Send email via SMTP
    try:
        if smtp_port == 465:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port)
        else:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()
        
        server.login(smtp_user, smtp_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
        print("Text email sent successfully")
    except Exception as e:
        print(f"Text email sending failed: {str(e)}")
        raise

# Task 3: Load quote into PostgreSQL
def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_quote', key='quote_data')

    conn = psycopg2.connect(
        dbname="dwh",
        user="ikeengr",
        password="DataEngineer247",
        host="89.40.0.150",
        port="5432"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS self_confidence (
            id SERIAL PRIMARY KEY,
            quote TEXT,
            author TEXT,
            type TEXT
        )
    """)

    cur.execute("""
        INSERT INTO self_confidence (quote, author, type)
        VALUES (%s, %s, %s)
    """, (data['quote'], data['author'], data['type']))

    conn.commit()
    cur.close()
    conn.close()

# Define tasks
fetch_task = PythonOperator(
    task_id='fetch_quote',
    python_callable=fetch_quote,
    provide_context=True,
    dag=dag
)

text_email_task = PythonOperator(
    task_id='send_text_email',
    python_callable=send_quote_as_text_email,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_quote',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag
)

# Set task dependencies
fetch_task >> [text_email_task, load_task]
