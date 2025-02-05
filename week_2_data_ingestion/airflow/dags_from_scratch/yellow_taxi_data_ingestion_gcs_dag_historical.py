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


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")


airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
url_prefix = "https://s3.amazonaws.com/nyc-tlc/trip+data"

yellow_taxi_url_template = url_prefix + "/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
yellow_taxi_output_file_template = airflow_home + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
yellow_taxi_parquet_file_template = airflow_home + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
yellow_taxi_gcs_path = "raw/yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

parquet_file = yellow_taxi_output_file_template.replace('.csv', '.parquet')


# Functions from the main.py file
def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
# The argument for spark is here
def upload_to_gcs(bucket, object_name, local_file):
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}
def download_parquetize_upload_dag(
        dag,
        url_template,
        local_csv_path_template,
        local_parquet_path_template,
        gcs_path_template
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f'curl -sSLf {url_template} > {local_csv_path_template}'
        )


        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{local_csv_path_template}",
                "dest_file": local_parquet_path_template
            },
        )


        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f'rm {local_csv_path_template} {local_parquet_path_template}'
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="yellow_taxi_data_dag_v2",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    max_active_runs=3,
    tags=['dtc-de'],
    start_date=datetime(2019, 1, 1),
    catchup=True,
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f'curl -sSLf {yellow_taxi_url_template} > {yellow_taxi_output_file_template}'
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{yellow_taxi_output_file_template}",
            "dest_file": yellow_taxi_parquet_file_template
        },
    )


    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": yellow_taxi_gcs_path,
            "local_file": yellow_taxi_parquet_file_template
        },
    )

    rm_task = BashOperator(
        task_id="rm_task",
        bash_command=f'rm {yellow_taxi_output_file_template} {yellow_taxi_parquet_file_template}'
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task