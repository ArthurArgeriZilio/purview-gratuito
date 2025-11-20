import logging
from azure.identity import DefaultAzureCredential
from azure.synapse.artifacts import ArtifactsClient

class SynapseCollector:
    def __init__(self):
        self.credential = DefaultAzureCredential()
        self.logger = logging.getLogger(__name__)

    def get_artifacts_client(self, workspace_name):
        endpoint = f"https://{workspace_name}.dev.azuresynapse.net"
        return ArtifactsClient(endpoint, self.credential)

    def scan_workspace_artifacts(self, workspace_name):
        """
        Scans a Synapse Workspace for internal artifacts: Pipelines, Notebooks, Datasets.
        """
        self.logger.info(f"Scanning Synapse Workspace: {workspace_name}")
        artifacts = {
            "pipelines": [],
            "notebooks": [],
            "datasets": [],
            "linked_services": []
        }
        
        try:
            client = self.get_artifacts_client(workspace_name)
            
            # 1. Pipelines
            pipelines = client.pipeline.get_pipelines_by_workspace()
            for p in pipelines:
                artifacts["pipelines"].append({
                    "id": f"{workspace_name}/pipeline/{p.name}",
                    "name": p.name,
                    "workspace": workspace_name
                })

            # 2. Notebooks
            notebooks = client.notebook.get_notebooks_by_workspace()
            for n in notebooks:
                artifacts["notebooks"].append({
                    "id": f"{workspace_name}/notebook/{n.name}",
                    "name": n.name,
                    "workspace": workspace_name
                })

            # 3. Datasets
            datasets = client.dataset.get_datasets_by_workspace()
            for d in datasets:
                artifacts["datasets"].append({
                    "id": f"{workspace_name}/dataset/{d.name}",
                    "name": d.name,
                    "type": d.properties.type,
                    "workspace": workspace_name
                })
                
            # 4. Linked Services (Connections)
            linked_services = client.linked_service.get_linked_services_by_workspace()
            for ls in linked_services:
                artifacts["linked_services"].append({
                    "id": f"{workspace_name}/linkedService/{ls.name}",
                    "name": ls.name,
                    "type": ls.properties.type,
                    "workspace": workspace_name
                })

        except Exception as e:
            self.logger.error(f"Failed to scan workspace {workspace_name}: {e}")
        
        return artifacts
