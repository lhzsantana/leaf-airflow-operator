from airflow import DAG
from airflow.operators.python import PythonOperator
from leaf_airflow_operator.operators.leaf_operator import LeafBatchUploadOperator
from datetime import datetime, timedelta
import requests
import os

ZIP_URL = "https://dev.fieldview.com/sample-agronomic-data/Planting_Harvesting_data_04_18_2018_21_46_18.zip"
LOCAL_ZIP_PATH = "/tmp/fieldview_data.zip"
LEAF_TOKEN = os.getenv("LEAF_API_TOKEN")  

def download_zip():
    response = requests.get(ZIP_URL)
    response.raise_for_status()
    with open(LOCAL_ZIP_PATH, "wb") as f:
        f.write(response.content)

with DAG(
    dag_id="prepare_fieldview_leaf_upload",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)}
) as dag:

    t_download = PythonOperator(
        task_id="download_fieldview_zip",
        python_callable=download_zip
    )

    t_upload = LeafBatchUploadOperator(
        task_id='upload_zip_to_leaf',
        file_path=LOCAL_ZIP_PATH,
        leaf_user_id='',
        destination_type='ISOXML',
        headers={'Authorization': f'Bearer {LEAF_TOKEN}'}
    )

    t_download >> t_upload