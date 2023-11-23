from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from extract import parse_url_to_fund, transform, load, BASE_PATH, FUNDO_ANGA
import os
import psycopg2


def parse_url_to_fund_task(**kwargs):
    scraped_data = parse_url_to_fund(FUNDO_ANGA)
    return scraped_data.to_dict()


def transform_task(**kwargs):
    transformed_data = transform(pd.DataFrame.from_dict(
        kwargs['ti'].xcom_pull(task_ids='parse_url_to_fund_task')))
    return transformed_data.to_dict()


# Task to load data
def load_task(**kwargs):
    supplementary_file_path = os.path.join(BASE_PATH,
                                           'Raw\\INFO_FUNDOS_ANGA_202307.xlsx')
    supplementary_file_df = pd.read_excel(supplementary_file_path)
    load(pd.DataFrame.from_dict(
        kwargs['ti'].xcom_pull(task_ids='transform_task')),
        supplementary_file_df)


# Task to create database table
def create_db_task(**kwargs):
    # Connect to PostgreSQL and create the table
    host = "localhost",
    dbname = "postgres",
    user = "gilzalc",
    password = "*********"

    conn = psycopg2.connect(dbname=kwargs['params']['dbname'],
                            user=kwargs['params']['user'],
                            password=kwargs['params']['password'],
                            host=kwargs['params']['host'])
    with conn.cursor() as cursor:
        with open("create.sql", "r") as f:
            create_sql = f.read()
        cursor.execute(create_sql)
    conn.commit()
    conn.close()


# Task to load data into the database
def load_into_db_task(**kwargs):
    csv_file_path = os.path.join(BASE_PATH, 'Refined\\final_data.csv')

    # Load CSV data into PostgreSQL
    with open(csv_file_path, 'r') as f:
        host = "localhost",
        dbname = "postgres",
        user = "gilzalc",
        password = "*********"

        # Connect to PostgreSQL and open a cursor
        conn = psycopg2.connect(dbname=kwargs['params']['dbname'],
                                user=kwargs['params']['user'],
                                password=kwargs['params']['password'],
                                host=kwargs['params']['host'])
        with conn.cursor() as cursor:
            # Copy data from CSV to the PostgreSQL table
            cursor.copy_from(f, 'fund_data', sep=',', null='')
        conn.commit()
        conn.close()


# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'params': {
        'host': 'localhost',
        'dbname': 'postgres',
        'user': 'gilzalc',
        'password': '*********',
    }
}

# Define the DAG
dag = DAG(
    'etl_process',
    default_args=default_args,
    description='ETL Process for Fund Information',
)

# Task to parse URL and extract data
parse_url_task = PythonOperator(
    task_id='parse_url_to_fund',
    python_callable=parse_url_to_fund_task,
    provide_context=True,
    dag=dag,
)

# Task to transform data
transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

# Task to load data
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    provide_context=True,
    dag=dag,
)

create_db_task = PythonOperator(
    task_id='create_db',
    python_callable=create_db_task,
    provide_context=True,
    dag=dag,
)

load_into_db_task = PythonOperator(
    task_id='load_db',
    python_callable=load_into_db_task,
    provide_context=True,
    dag=dag,
)
var = parse_url_task >> transform_task >> load_data_task >> create_db_task >> load_into_db_task
