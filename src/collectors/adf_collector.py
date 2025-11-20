import logging
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

class ADFCollector:
    def __init__(self, subscription_id):
        self.credential = DefaultAzureCredential()
        self.client = DataFactoryManagementClient(self.credential, subscription_id)
        self.logger = logging.getLogger(__name__)

    def scan_adf(self, resource_group_name, factory_name):
        """
        Scans an Azure Data Factory for Pipelines, Datasets, and Linked Services.
        """
        self.logger.info(f"Scanning ADF: {factory_name}")
        artifacts = {
            "pipelines": [],
            "datasets": [],
            "linked_services": []
        }

        try:
            # 1. Pipelines
            pipelines = self.client.pipelines.list_by_factory(resource_group_name, factory_name)
            for p in pipelines:
                artifacts["pipelines"].append({
                    "id": p.id,
                    "name": p.name,
                    "factory_name": factory_name
                })

            # 2. Datasets
            datasets = self.client.datasets.list_by_factory(resource_group_name, factory_name)
            for d in datasets:
                # Try to get type (e.g., AzureSqlTable, Parquet)
                ds_type = d.properties.type if hasattr(d.properties, 'type') else "Unknown"
                artifacts["datasets"].append({
                    "id": d.id,
                    "name": d.name,
                    "type": ds_type,
                    "factory_name": factory_name
                })

            # 3. Linked Services
            linked_services = self.client.linked_services.list_by_factory(resource_group_name, factory_name)
            for ls in linked_services:
                ls_type = ls.properties.type if hasattr(ls.properties, 'type') else "Unknown"
                artifacts["linked_services"].append({
                    "id": ls.id,
                    "name": ls.name,
                    "type": ls_type,
                    "factory_name": factory_name
                })

        except Exception as e:
            self.logger.error(f"Failed to scan ADF {factory_name}: {e}")

        return artifacts
