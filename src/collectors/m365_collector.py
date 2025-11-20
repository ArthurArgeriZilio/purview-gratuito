import asyncio
from azure.identity import DefaultAzureCredential
from msgraph import GraphServiceClient
from msgraph.generated.users.users_request_builder import UsersRequestBuilder

class M365Collector:
    def __init__(self):
        # Using DefaultAzureCredential. 
        # Ensure you have 'User.Read.All', 'Group.Read.All' permissions granted to the identity being used.
        self.credential = DefaultAzureCredential()
        self.scopes = ['https://graph.microsoft.com/.default']
        self.client = GraphServiceClient(credentials=self.credential, scopes=self.scopes)

    async def get_all_users(self):
        """
        Fetches all users from Microsoft 365.
        """
        try:
            users = await self.client.users.get()
            if users and users.value:
                return [
                    {
                        "id": user.id,
                        "displayName": user.display_name,
                        "mail": user.mail,
                        "userPrincipalName": user.user_principal_name
                    }
                    for user in users.value
                ]
            return []
        except Exception as e:
            print(f"Error fetching users: {e}")
            return []

    async def get_all_groups(self):
        """
        Fetches all groups (Teams, M365 Groups, Security Groups).
        """
        try:
            groups = await self.client.groups.get()
            if groups and groups.value:
                return [
                    {
                        "id": group.id,
                        "displayName": group.display_name,
                        "groupTypes": group.group_types,
                        "mail": group.mail
                    }
                    for group in groups.value
                ]
            return []
        except Exception as e:
            print(f"Error fetching groups: {e}")
            return []
