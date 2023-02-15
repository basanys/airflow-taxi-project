import os

from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
GCP_PROJECT = os.getenv('GCP_PROJECT')
GCP_BUCKET = os.getenv('GCP_BUCKET')
GCP_BQ_DATASET = os.getenv('GCP_BQ_DATASET') 

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2022, 5, 1),
    end_date=datetime(2022, 6, 1)
)

# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet


URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data' 
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
parquet_file = 'output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

with local_workflow:
    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    local_to_gcs = LocalFilesystemToGCSOperator(
        task_id="local_to_gcs",
        src=OUTPUT_FILE_TEMPLATE,
        dst="raw/",
        bucket="zoomcamp_lake_zoomcamp-361206",
    )

    gcs_to_bq = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        table_resource = {
            "tableReference" : {
                  "projectId": GCP_PROJECT,
                  "datasetId": GCP_BQ_DATASET,
                  "tableId": TABLE_NAME_TEMPLATE
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{GCP_BUCKET}/raw/{parquet_file}"],
            },
        },
    )
    

    wget_task >> local_to_gcs