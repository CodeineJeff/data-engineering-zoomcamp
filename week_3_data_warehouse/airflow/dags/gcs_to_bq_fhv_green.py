import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="green_gcs_to_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    gcs_to_gcs = GCSToGCSOperator(
        task_id="green_gcs_to_gcs_task",
        source_bucket=BUCKET,
        source_objects=['raw/green_tripdata/2019/*.parquet', 'raw/green_tripdata/2020/*.parquet',
                        'raw/green_tripdata/2021/*.parquet'],
        destination_bucket=BUCKET,
        delimiter='.parquet',
        move_object=True,
        destination_object='green_tripdata_all/',
    )

    gcs_to_bq_ext = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_green_tripdata_all_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/green_tripdata_all/*"],
            },
        },
    )

    CREATE_PART_TABLE_QUERY = f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.green_tripdata_partitioned \
        PARTITION BY DATE(lpep_pickup_datetime) AS\
        SELECT * EXCEPT (ehail_fee) FROM {BIGQUERY_DATASET}.external_green_tripdata_all_table;"

    bq_ext_to_part = BigQueryInsertJobOperator(
        task_id="bq_ext_to_part",
        configuration={
            "query": {
                "query": CREATE_PART_TABLE_QUERY,
                "useLegacySql": False,
            },
        },
    )

    gcs_to_gcs >> gcs_to_bq_ext >> bq_ext_to_part