import requests
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# ----------------------------
# Step 1: Extract - API Request
# ----------------------------

def extract_data(**kwargs):
    url = "https://game-quiz.p.rapidapi.com/quiz/random"
    querystring = {"lang": "en", "amount": "100", "format": "mcq", "cached": "false", "endpoint": "games"}

    headers = {
        "x-rapidapi-key": "e7cbee4ae1mshd0704b6dcc65548p1632e3jsncb04d537a049",
        "x-rapidapi-host": "game-quiz.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code == 200:
        response_data = response.json()['data']  # Extract 'data' from the JSON response
        # Push the extracted data to XCom for the next task
        ti = kwargs['ti']
        ti.xcom_push(key='response_data', value=response_data)
    else:
        raise Exception(f"API request failed with status code: {response.status_code}")

# ----------------------------
# Step 2: Transform - Data Transformation
# ----------------------------

def transform_data(**kwargs):
    ti = kwargs['ti']
    response_data = ti.xcom_pull(task_ids='extract_data', key='response_data')

    if response_data:
        # Convert to DataFrame
        df = pd.json_normalize(response_data)

        # Debug: Print DataFrame shape and columns
        print("DataFrame shape:", df.shape)
        print("DataFrame columns:", df.columns)

        # Select and transform the columns, ensuring we have the correct number of columns
        # Here, we will check the structure and handle optional columns if they exist
        required_columns = [
            'question', 'options.correct', 'options.incorrect', 'reference', 'extra.type', 'extra.content', 'options.is_image'
        ]

        # Ensure these columns exist before processing
        available_columns = [col for col in required_columns if col in df.columns]

        # Extract the relevant data, filling any missing values
        df_transformed = df[available_columns].fillna("")

        # Debug: Print the transformed DataFrame
        print("Transformed DataFrame:")
        print(df_transformed.head())

        # Assign the correct column names to match the final structure
        column_mapping = {
            'question': 'question',
            'options.correct': 'correct_answer',
            'options.incorrect': 'option_1',  # This could be split into multiple columns if needed
            'reference': 'reference',
            'extra.type': 'extra_type',
            'extra.content': 'extra_content',
            'options.is_image': 'is_image'
        }

        df_transformed = df_transformed.rename(columns=column_mapping)

        # Ensure the correct number of columns in the final DataFrame
        print("Final DataFrame shape:", df_transformed.shape)

        # Push the transformed DataFrame to XCom
        ti.xcom_push(key='df_data', value=df_transformed)
    else:
        raise ValueError("No data extracted from the API!")

# ----------------------------
# Step 3: Load - Data to PostgreSQL
# ----------------------------

def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    df_transformed = ti.xcom_pull(task_ids='transform_data', key='df_data')

    if df_transformed is not None and not df_transformed.empty:
        # Database connection parameters
        db_params = {
            'dbname': 'dwh',
            'user': 'ikeengr',
            'password': 'DataEngineer247',
            'host': '89.40.0.150',
            'port': '5432'
        }

        try:
            # Establish connection to the PostgreSQL database
            conn = psycopg2.connect(**db_params)
            cursor = conn.cursor()

            # Prepare the SQL query to insert the transformed data into the database
            insert_query = """
                INSERT INTO random_mcq_game_quiz (
                    question, correct_answer, option_1, option_2, option_3, option_4, reference, extra_type, extra_content, is_image
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Insert each row of the DataFrame into the database
            for index, row in df_transformed.iterrows():
                cursor.execute(insert_query, tuple(row))

            # Commit the transaction
            conn.commit()

            print(f"Successfully inserted {len(df_transformed)} rows into the PostgreSQL database.")

        except Exception as e:
            print(f"Error loading data to PostgreSQL: {e}")
        finally:
            # Close the database connection
            if conn:
                conn.close()
            if cursor:
                cursor.close()
    else:
        print("No transformed data to load into PostgreSQL.")

# ----------------------------
# Airflow DAG Configuration
# ----------------------------

with DAG(
    'random_mcq_game_quiz', 
    default_args={
        'owner': 'Ik',
        'start_date': days_ago(1),  # Start the DAG from 1 day ago (adjust as needed)
        'retries': 1,
    },
    schedule_interval='@daily',  # Run the DAG daily
    catchup=False,  # Don't run missed DAG runs
) as dag:
    
    # Task 1: Extract data from the API
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    # Task 2: Transform the data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    # Task 3: Load the data into PostgreSQL
    load_task = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres,
        provide_context=True,
    )

    # Define task dependencies
    extract_task >> transform_task >> load_task
