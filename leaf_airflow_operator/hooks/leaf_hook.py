import requests

class LeafHook:
    base_url = "https://api.withleaf.io"

    def run(self, endpoint, method, headers, data=None):
        url = f"{self.base_url}{endpoint}"
        resp = requests.request(method=method, url=url, headers=headers, json=data)
        resp.raise_for_status()
        return resp.json()

    def upload_batch_file(self, file_path, leaf_user_id, provider, headers):
        url = f"{self.base_url}/services/operations/api/batch"
        params = {
            "leafUserId": leaf_user_id,
            "provider": provider
        }
        with open(file_path, 'rb') as f:
            files = {'file': f}
            resp = requests.post(url, headers=headers, files=files, params=params)
            resp.raise_for_status()
            return resp.json()