from airflow.sensors.base import BaseSensorOperator
from leaf_airflow_operator.hooks.leaf_hook import LeafHook

class LeafConversionStatusSensor(BaseSensorOperator):
    def __init__(self, conversion_id, expected_status='RECEIVED', headers=None, **kwargs):
        super().__init__(**kwargs)
        self.conversion_id = conversion_id
        self.expected_status = expected_status
        self.headers = headers or {}

    def poke(self, context):
        hook = LeafHook()
        status_response = hook.run(f"/v1/conversions/{self.conversion_id}", method="GET", headers=self.headers)
        status = status_response.get("status")
        self.log.info(f"Current status for {self.conversion_id}: {status}")
        return status == self.expected_status
