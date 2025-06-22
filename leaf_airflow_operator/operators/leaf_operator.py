from airflow.models import BaseOperator
from leaf_airflow_operator.hooks.leaf_hook import LeafHook

class LeafBatchUploadOperator(BaseOperator):
    def __init__(self, file_path, leaf_user_id, provider="Other", headers=None, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.leaf_user_id = leaf_user_id
        self.provider = provider
        self.headers = headers or {}

    def execute(self, context):
        hook = LeafHook()
        return hook.upload_batch_file(
            file_path=self.file_path,
            leaf_user_id=self.leaf_user_id,
            provider=self.provider,
            headers=self.headers
        )