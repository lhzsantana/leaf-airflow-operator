from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from leaf_airflow_operator.operators.leaf_operator import LeafBatchUploadOperator, LeafAuthenticateOperator
from leaf_airflow_operator.sensors.leaf_sensor import LeafBatchStatusSensor
from datetime import datetime, timedelta
import requests
import os

ZIP_URL = "https://dev.fieldview.com/sample-agronomic-data/Planting_Harvesting_data_04_18_2018_21_46_18.zip"
LOCAL_ZIP_PATH = "/tmp/fieldview_data.zip"

def download_zip():
    response = requests.get(ZIP_URL)
    response.raise_for_status()
    with open(LOCAL_ZIP_PATH, "wb") as f:
        f.write(response.content)

with DAG(
    dag_id="_leaf_file_upload",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)}
) as dag:

    t_download = PythonOperator(
        task_id="download_fieldview_zip",
        python_callable=download_zip
    )

    t_auth = LeafAuthenticateOperator(
        task_id='auth_leaf',
        username = Variable.get("LEAF_USERNAME"),
        password = Variable.get("LEAF_PASSWORD")
    )

    t_upload = LeafBatchUploadOperator(
        task_id='upload_zip_to_leaf',
        file_path=LOCAL_ZIP_PATH,
        leaf_user_id='5c0542dd-bf20-4ece-a358-ebf0341617a9'
    )

    t_wait = LeafBatchStatusSensor(
        task_id="wait_batch_processed",
        upload_task_id="upload_zip_to_leaf",
        expected_status="PROCESSED",
        headers={"Authorization": "Bearer {{ ti.xcom_pull(task_ids='auth_leaf')['access_token'] }}"},
        poke_interval=30,
        timeout=1800,
    )

    t_download >> t_auth >> t_upload >> t_wait