from airflow import DAG
from airflow.models import Variable

import uuid

from leaf_airflow_operator.operators.leaf_operator import LeafAuthenticateOperator, LeafCreateSatelliteFieldOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="_leaf_historical_satellite",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)}
) as dag:

    t_auth = LeafAuthenticateOperator(
        task_id='auth_leaf',
        username = Variable.get("LEAF_USERNAME"),
        password = Variable.get("LEAF_PASSWORD")
    )

    t_create_field = LeafCreateSatelliteFieldOperator(
        task_id="create_field",
        external_id=str(uuid.uuid4()),
        geometry={
            "type": "MultiPolygon",
            "coordinates": [[[
                [-47.9292, -15.7801],
                [-47.9292, -15.7811],
                [-47.9282, -15.7811],
                [-47.9282, -15.7801],
                [-47.9292, -15.7801]
            ]]]
        },
        providers=["sentinel"],
        days_before=1
    )

    t_auth >> t_create_field