import kuzu
import os
import logging

class DBManager:
    def __init__(self, db_path="./purview_db"):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        
        # Initialize database
        self.db = kuzu.Database(db_path)
        self.conn = kuzu.Connection(self.db)
        self._init_schema()

    def _init_schema(self):
        """
        Defines the Graph Schema (Nodes and Relationships).
        """
        self.logger.info("Initializing Database Schema...")
        
        # --- Nodes ---
        self._create_node_table("User", "id STRING, displayName STRING, mail STRING, upn STRING", "id")
        self._create_node_table("M365Group", "id STRING, displayName STRING, mail STRING", "id")
        self._create_node_table("Subscription", "id STRING, name STRING, state STRING", "id")
        self._create_node_table("Resource", "id STRING, name STRING, type STRING, location STRING, resourceGroup STRING", "id")

        # --- Relationships (Edges) ---
        self._create_rel_table("MemberOf", "FROM User TO M365Group")
        self._create_rel_table("Contains", "FROM Subscription TO Resource")
        
        # Note: 'HasAccess' would require complex RBAC analysis, skipping for initial version.

    def _create_node_table(self, name, schema, primary_key):
        try:
            self.conn.execute(f"CREATE NODE TABLE {name}({schema}, PRIMARY KEY ({primary_key}))")
        except RuntimeError as e:
            if "already exists" not in str(e):
                self.logger.warning(f"Could not create node table {name}: {e}")

    def _create_rel_table(self, name, schema):
        try:
            self.conn.execute(f"CREATE REL TABLE {name}({schema})")
        except RuntimeError as e:
            if "already exists" not in str(e):
                self.logger.warning(f"Could not create rel table {name}: {e}")

    def upsert_user(self, user_data):
        # Kuzu doesn't have standard UPSERT yet in all versions, so we use MERGE logic or DELETE/INSERT
        # For simplicity in this demo, we'll try to insert and ignore errors or use MERGE if supported.
        # Kuzu Cypher supports MERGE.
        query = """
        MERGE (u:User {id: $id})
        SET u.displayName = $displayName, u.mail = $mail, u.upn = $upn
        """
        self.conn.execute(query, parameters={
            "id": user_data.get("id"),
            "displayName": user_data.get("displayName"),
            "mail": user_data.get("mail", ""),
            "upn": user_data.get("userPrincipalName")
        })

    def upsert_resource(self, res_data):
        query = """
        MERGE (r:Resource {id: $id})
        SET r.name = $name, r.type = $type, r.location = $location, r.resourceGroup = $resourceGroup
        """
        self.conn.execute(query, parameters={
            "id": res_data.get("id"),
            "name": res_data.get("name"),
            "type": res_data.get("type"),
            "location": res_data.get("location"),
            "resourceGroup": res_data.get("resourceGroup")
        })

    def upsert_subscription(self, sub_data):
        query = """
        MERGE (s:Subscription {id: $id})
        SET s.name = $name
        """
        self.conn.execute(query, parameters={
            "id": sub_data.get("subscriptionId"),
            "name": sub_data.get("displayName")
        })
        
    def link_subscription_resource(self, sub_id, resource_id):
        query = """
        MATCH (s:Subscription {id: $sub_id}), (r:Resource {id: $res_id})
        MERGE (s)-[:Contains]->(r)
        """
        self.conn.execute(query, parameters={"sub_id": sub_id, "res_id": resource_id})

    def get_graph_stats(self):
        users = self.conn.execute("MATCH (u:User) RETURN count(u)").get_next()[0]
        resources = self.conn.execute("MATCH (r:Resource) RETURN count(r)").get_next()[0]
        return {"users": users, "resources": resources}
