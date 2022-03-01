import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

from yellow_taxi_data_ingestion_gcs_dag_historical import download_parquetize_upload_dag

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
url_prefix = "https://s3.amazonaws.com/nyc-tlc/trip+data"

fhv_url_template = url_prefix + "/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
fhv_output_file_template = airflow_home + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
fhv_parquet_file_template = airflow_home + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
fhv_gcs_path = "raw/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

fhv_data = DAG(
    dag_id="fhv_data_ingestion_gcs",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    max_active_runs=3,
    tags=['dtc-de'],
    start_date=datetime(2019, 1, 1),
    catchup=True,
)

download_parquetize_upload_dag(
        dag =fhv_data,
        url_template=fhv_url_template,
        local_csv_path_template=fhv_output_file_template,
        local_parquet_path_template=fhv_parquet_file_template,
        gcs_path_template=fhv_gcs_path
)