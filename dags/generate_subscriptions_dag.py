from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG('create_subscriptions', default_args=default_args, schedule_interval=None) as dag:
    spark_job_task = SparkSubmitOperator(
        task_id='create_file',
        conn_id='spark_default',
        application='/opt/airflow/dags/spark_jobs/subscription_parquet_job.py',
        verbose=False,
        dag=dag
    )
    spark_job_task