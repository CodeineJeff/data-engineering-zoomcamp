from airflow import DAG
import os
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

from ingest_script import ingest_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

local_op = DAG(
    "LocalIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1)
)

url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"


url_prefix = "https://s3.amazonaws.com/nyc-tlc/trip+data"
url_template = url_prefix + "/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
output_file_template = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
table_name_template = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'


with local_op:
    wget_task = BashOperator(
        task_id='curl',
        bash_command=f'curl -sSL {url_template} > {output_file_template}'
    )

    ingest_task = PythonOperator(
        task_id='ingest',
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=table_name_template,
            csv_file=output_file_template
        )
    )

    wget_task >> ingest_task