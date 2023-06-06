from datetime import datetime, timedelta
from airflow import DAG
import json
from faker import Faker
import random
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta
import os

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

filesToGenerate = Variable.get('filesToGenerate', default_var=3)
startDate = Variable.get('startDate', default_var = date.today().strftime("%Y-%m-%d"))

def generateData():

    current_date = datetime.strptime(startDate, "%Y-%m-%d").date()

    for n in range(0,filesToGenerate):

        dateFolder = (current_date + timedelta(days=n)).strftime("%Y-%m-%d")
        random_ints = random.sample(range(1, 251), 100)
        result=[]

        for i in random_ints:

            data = {
                "id": i,
                "active": random.choice([True, False]),
                "subscription": random.choice(["Basic", "Premium", "Ultimate","Sports","Entertainment","News","Movies","Family","Premium Plus","Custom"]),
                "customer_first_name": fake.first_name(),
                "customer_last_name": fake.last_name(),
                "cost": random.randint(50, 100),
                "start_date": str(fake.date_between(start_date="-30d", end_date="+30d")),
                "end_date": str(fake.date_between(start_date="+30d", end_date="+365d"))
            }
            
            result.append(data)
        
        result = sorted(result, key=lambda x: x["id"])

        if os.path.exists(f'/opt/airflow/data/files/registers/date={dateFolder}'):
            os.system("rm -r " + f'/opt/airflow/data/files/registers/date={dateFolder}')
        os.makedirs(f'/opt/airflow/data/files/registers/date={dateFolder}')

        with open(f"/opt/airflow/data/files/registers/date={dateFolder}/data.json", "w") as f:
            for obj in result:
                json.dump(obj, f)
                f.write('\n')

with DAG('create_json_files', default_args=default_args, schedule_interval=None) as dag:

    createJSONData = PythonOperator(
        task_id='createJSONData',
        python_callable=generateData,
        dag=dag
    )

    createJSONData
