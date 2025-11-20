import requests
import time
import msal
import logging

class PowerBICollector:
    def __init__(self, client_id, client_secret, tenant_id):
        self.authority = f"https://login.microsoftonline.com/{tenant_id}"
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = ["https://analysis.windows.net/powerbi/api/.default"]
        self.base_url = "https://api.powerbi.com/v1.0/myorg"
        self.headers = None

    def authenticate(self):
        """Authenticates using MSAL and sets the authorization header."""
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=self.authority,
            client_credential=self.client_secret
        )
        result = app.acquire_token_for_client(scopes=self.scope)
        
        if "access_token" in result:
            self.headers = {
                "Authorization": f"Bearer {result['access_token']}",
                "Content-Type": "application/json"
            }
            logging.info("Successfully authenticated with Power BI.")
        else:
            raise Exception(f"Authentication failed: {result.get('error_description')}")

    def get_all_workspaces(self):
        """Fetches all workspace IDs using the Admin API."""
        url = f"{self.base_url}/admin/groups?$top=5000"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        workspaces = response.json().get("value", [])
        return [w["id"] for w in workspaces]

    def run_metadata_scan(self, workspace_ids):
        """
        Orchestrates the Scanner API process:
        1. Chunks workspaces into batches of 100.
        2. Initiates scan for each batch.
        3. Polls for status.
        4. Retrieves results.
        """
        # Chunk workspace IDs into batches of 100 (API limit)
        chunks = [workspace_ids[i:i + 100] for i in range(0, len(workspace_ids), 100)]
        all_results = []

        for chunk in chunks:
            scan_id = self._initiate_scan(chunk)
            if scan_id:
                if self._wait_for_scan(scan_id):
                    result = self._get_scan_result(scan_id)
                    all_results.append(result)
        
        return all_results

    def _initiate_scan(self, workspace_ids):
        """Step 2: Initiate the scan."""
        url = f"{self.base_url}/admin/workspaces/getInfo?lineage=True&datasourceDetails=True&datasetSchema=True&getArtifactUsers=True"
        body = {"workspaces": workspace_ids}
        
        response = requests.post(url, headers=self.headers, json=body)
        if response.status_code == 202:
            return response.json()["id"]
        else:
            logging.error(f"Failed to initiate scan: {response.text}")
            return None

    def _wait_for_scan(self, scan_id):
        """Step 3: Poll for scan status."""
        url = f"{self.base_url}/admin/workspaces/scanStatus/{scan_id}"
        
        while True:
            response = requests.get(url, headers=self.headers)
            status = response.json().get("status")
            
            if status == "Succeeded":
                return True
            elif status == "Failed":
                logging.error(f"Scan failed for ID {scan_id}")
                return False
            
            time.sleep(2) # Wait before polling again

    def _get_scan_result(self, scan_id):
        """Step 4: Get the scan result."""
        url = f"{self.base_url}/admin/workspaces/scanResult/{scan_id}"
        response = requests.get(url, headers=self.headers)
        return response.json()
