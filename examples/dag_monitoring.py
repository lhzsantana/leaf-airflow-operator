from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from leaf_airflow_operator.operators.leaf_operator import (
    LeafAuthenticateOperator,
    LeafCreateFieldOperator,
    LeafCreateSatelliteFieldOperator,
    LeafBatchUploadOperator
)
from leaf_airflow_operator.sensors.leaf_sensor import LeafBatchStatusSensor
from datetime import datetime, timedelta
import uuid

ZIP_URL = "https://dev.fieldview.com/sample-agronomic-data/Planting_Harvesting_data_04_18_2018_21_46_18.zip"
LOCAL_ZIP_PATH = "/tmp/fieldview_data.zip"
LEAF_USER_ID='5c0542dd-bf20-4ece-a358-ebf0341617a9'

def download_zip():
    response = requests.get(ZIP_URL)
    response.raise_for_status()
    with open(LOCAL_ZIP_PATH, "wb") as f:
        f.write(response.content)

polygon = {
            "type": "MultiPolygon",
            "coordinates": [[[
                [-47.9292, -15.7801],
                [-47.9292, -15.7811],
                [-47.9282, -15.7811],
                [-47.9282, -15.7801],
                [-47.9292, -15.7801]
            ]]]
        }

with DAG(
    dag_id="_leaf_monitoring",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)}
) as dag:

    t_auth = LeafAuthenticateOperator(
        task_id='auth_leaf',
        username = Variable.get("LEAF_USERNAME"),
        password = Variable.get("LEAF_PASSWORD")
    )

    branch = DummyOperator(task_id="branch_after_auth")
    field_name = str(uuid.uuid4())

    t_create_field = LeafCreateFieldOperator(
        task_id="create_field",
        name=field_name,
        geometry=polygon,
        leaf_user_id=LEAF_USER_ID
    )

    t_create_satellite = LeafCreateSatelliteFieldOperator(
        task_id="create_satellite_field",
        external_id=field_name,
        geometry=polygon,
        providers=["sentinel"],
        days_before=1
    )

    t_upload = LeafBatchUploadOperator(
        task_id='upload_zip_to_leaf',
        file_path=LOCAL_ZIP_PATH,
        leaf_user_id=LEAF_USER_ID
    )

    t_wait = LeafBatchStatusSensor(
        task_id="wait_batch_processed",
        upload_task_id="upload_zip_to_leaf",
        expected_status="PROCESSED",
        headers={"Authorization": "Bearer {{ ti.xcom_pull(task_ids='auth_leaf')['access_token'] }}"},
        poke_interval=30,
        timeout=1800,
    )

    t_auth >> branch
    branch >> [t_create_field, t_create_satellite, t_upload]
    t_upload >> t_wait
