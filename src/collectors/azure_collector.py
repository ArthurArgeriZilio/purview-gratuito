import logging
from azure.identity import DefaultAzureCredential
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest

class AzureCollector:
    def __init__(self):
        self.credential = DefaultAzureCredential()
        self.client = ResourceGraphClient(self.credential)
        self.logger = logging.getLogger(__name__)

    def run_query(self, query: str, subscriptions: list = None):
        """
        Executes a Kusto Query Language (KQL) query against Azure Resource Graph.
        """
        try:
            request = QueryRequest(
                query=query,
                subscriptions=subscriptions
            )
            response = self.client.resources(request)
            return response.data
        except Exception as e:
            self.logger.error(f"Error running query: {e}")
            raise

    def get_all_resources(self):
        """
        Fetches a broad inventory of Azure resources.
        """
        query = """
        Resources
        | project id, name, type, location, tags, subscriptionId, resourceGroup, properties
        | limit 5000
        """
        self.logger.info("Fetching all Azure resources...")
        return self.run_query(query)

    def get_subscriptions(self):
        """
        Fetches all subscriptions accessible to the user.
        """
        query = """
        resourcecontainers
        | where type == 'microsoft.resources/subscriptions'
        | project subscriptionId, name, type, location, tags
        """
        self.logger.info("Fetching subscriptions...")
        return self.run_query(query)
