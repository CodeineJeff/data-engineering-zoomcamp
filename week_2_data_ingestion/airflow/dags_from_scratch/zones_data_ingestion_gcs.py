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

zones_url_template = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
zones_output_file_template = airflow_home + '/taxi_zone_lookup.csv'
zones_parquet_file_template = airflow_home + '/taxi_zone_lookup.parquet'
zones_gcs_path = "raw/taxi_zone/taxi_zone_lookup.parquet"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

zones_data = DAG(
    dag_id="zones_data_ingestion_gcs",
    schedule_interval="0 8 2 * *",
    default_args=default_args,
    max_active_runs=1,
    tags=['dtc-de'],
    start_date=days_ago(1),
    catchup=False,
)

download_parquetize_upload_dag(
        dag=zones_data,
        url_template=zones_url_template,
        local_csv_path_template=zones_output_file_template,
        local_parquet_path_template=zones_parquet_file_template,
        gcs_path_template=zones_gcs_path
)