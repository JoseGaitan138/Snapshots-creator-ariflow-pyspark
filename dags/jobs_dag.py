from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from faker import Faker
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import pytest

fake = Faker()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def generateSuscriptions():
    subscriptions = [
        {"subscription": "Basic", "numberOfChannels": 50, "extras": {}},
        {"subscription": "Premium", "numberOfChannels": 100, "extras": {"HBO": "4", "Cinemax": "3"}},
        {"subscription": "Ultimate", "numberOfChannels": 200, "extras": {"HBO": "5", "Cinemax": "4", "Showtime": "3", "Sports Package": "2"}},
        {"subscription": "Sports", "numberOfChannels": 75, "extras": {"Sports Package": "5"}},
        {"subscription": "Entertainment", "numberOfChannels": 75, "extras": {"Showtime": "4", "Kids Package": "3"}},
        {"subscription": "News", "numberOfChannels": 50, "extras": {"CNN": "5", "Fox News": "3"}},
        {"subscription": "Movies", "numberOfChannels": 100, "extras": {"HBO": "5", "Cinemax": "4", "Showtime": "3"}},
        {"subscription": "Family", "numberOfChannels": 75, "extras": {"Kids Package": "5", "DVR": "3"}},
        {"subscription": "Premium Plus", "numberOfChannels": 200, "extras": {"HBO": "5", "Cinemax": "4", "Showtime": "4", "Sports Package": "3", "DVR": "2"}},
        {"subscription": "Custom", "numberOfChannels": 150, "extras": {}}
    ]
    df = pd.DataFrame(subscriptions)
    print(df)
    pq.write_table(pa.Table.from_pandas(df), '/opt/airflow/data/files/subscription/subscriptions.parquet')

with DAG('snapshots_dag', default_args=default_args, schedule_interval=None) as dag:
    spark_job_task = SparkSubmitOperator(
        task_id='create_snapshoots',
        conn_id='spark_default',
        application='/opt/airflow/dags/spark_jobs/create_snapshoots_job.py',
        verbose=False,
        dag=dag
    )
    spark_job_task