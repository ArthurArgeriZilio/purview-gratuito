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
        
        # --- Power BI Nodes ---
        self._create_node_table("PBIWorkspace", "id STRING, name STRING, state STRING, capacityId STRING", "id")
        self._create_node_table("PBIReport", "id STRING, name STRING, datasetId STRING", "id")
        self._create_node_table("PBIDataset", "id STRING, name STRING, configuredBy STRING", "id")

        # --- Relationships (Edges) ---
        self._create_rel_table("MemberOf", "FROM User TO M365Group")
        self._create_rel_table("Contains", "FROM Subscription TO Resource")
        
        # --- Power BI Edges ---
        self._create_rel_table("PBIContains", "FROM PBIWorkspace TO PBIReport")
        self._create_rel_table("PBIContainsDataset", "FROM PBIWorkspace TO PBIDataset")
        self._create_rel_table("PBIFeeds", "FROM PBIDataset TO PBIReport")

        # --- Synapse Nodes ---
        self._create_node_table("SynapsePipeline", "id STRING, name STRING, workspace STRING", "id")
        self._create_node_table("SynapseNotebook", "id STRING, name STRING, workspace STRING", "id")
        self._create_node_table("SynapseDataset", "id STRING, name STRING, type STRING, workspace STRING", "id")
        self._create_node_table("SynapseLinkedService", "id STRING, name STRING, type STRING, workspace STRING", "id")

        # --- Synapse Edges ---
        # We link these to the generic 'Resource' node which represents the Workspace
        self._create_rel_table("SynapseContains", "FROM Resource TO SynapsePipeline")
        self._create_rel_table("SynapseContainsNotebook", "FROM Resource TO SynapseNotebook")
        self._create_rel_table("SynapseContainsDataset", "FROM Resource TO SynapseDataset")
        self._create_rel_table("SynapseContainsLinkedService", "FROM Resource TO SynapseLinkedService")

        # --- ADF Nodes ---
        self._create_node_table("ADFPipeline", "id STRING, name STRING, factory STRING", "id")
        self._create_node_table("ADFDataset", "id STRING, name STRING, type STRING, factory STRING", "id")
        self._create_node_table("ADFLinkedService", "id STRING, name STRING, type STRING, factory STRING", "id")

        # --- ADF Edges ---
        self._create_rel_table("ADFContainsPipeline", "FROM Resource TO ADFPipeline")
        self._create_rel_table("ADFContainsDataset", "FROM Resource TO ADFDataset")
        self._create_rel_table("ADFContainsLinkedService", "FROM Resource TO ADFLinkedService")

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

    def upsert_pbi_workspace(self, ws_data):
        query = """
        MERGE (w:PBIWorkspace {id: $id})
        SET w.name = $name, w.state = $state, w.capacityId = $capacityId
        """
        self.conn.execute(query, parameters={
            "id": ws_data.get("id"),
            "name": ws_data.get("name"),
            "state": ws_data.get("state"),
            "capacityId": ws_data.get("capacityId", "")
        })

    def upsert_pbi_report(self, report_data, workspace_id):
        # Upsert Report
        query = """
        MERGE (r:PBIReport {id: $id})
        SET r.name = $name, r.datasetId = $datasetId
        """
        self.conn.execute(query, parameters={
            "id": report_data.get("id"),
            "name": report_data.get("name"),
            "datasetId": report_data.get("datasetId", "")
        })
        
        # Link Workspace -> Report
        link_query = """
        MATCH (w:PBIWorkspace {id: $ws_id}), (r:PBIReport {id: $r_id})
        MERGE (w)-[:PBIContains]->(r)
        """
        self.conn.execute(link_query, parameters={"ws_id": workspace_id, "r_id": report_data.get("id")})

    def upsert_pbi_dataset(self, dataset_data, workspace_id):
        # Upsert Dataset
        query = """
        MERGE (d:PBIDataset {id: $id})
        SET d.name = $name, d.configuredBy = $configuredBy
        """
        self.conn.execute(query, parameters={
            "id": dataset_data.get("id"),
            "name": dataset_data.get("name"),
            "configuredBy": dataset_data.get("configuredBy", "")
        })
        
        # Link Workspace -> Dataset
        link_query = """
        MATCH (w:PBIWorkspace {id: $ws_id}), (d:PBIDataset {id: $d_id})
        MERGE (w)-[:PBIContainsDataset]->(d)
        """
        self.conn.execute(link_query, parameters={"ws_id": workspace_id, "d_id": dataset_data.get("id")})

    def link_dataset_to_report(self, dataset_id, report_id):
        if not dataset_id or not report_id:
            return
        query = """
        MATCH (d:PBIDataset {id: $d_id}), (r:PBIReport {id: $r_id})
        MERGE (d)-[:PBIFeeds]->(r)
        """
        self.conn.execute(query, parameters={"d_id": dataset_id, "r_id": report_id})

    def upsert_synapse_artifacts(self, workspace_resource_id, artifacts):
        # We need the workspace name to link, but the Resource ID is what we have in the DB as the 'Resource' node.
        # However, the artifacts have 'workspace' name.
        # We assume the 'Resource' node exists with the full ID.
        # We need to find the Resource node that matches the workspace name to link correctly.
        # Or simpler: The caller passes the Resource ID of the workspace.
        
        # 1. Pipelines
        for p in artifacts.get("pipelines", []):
            self.conn.execute("MERGE (n:SynapsePipeline {id: $id}) SET n.name = $name, n.workspace = $ws", 
                              parameters={"id": p["id"], "name": p["name"], "ws": p["workspace"]})
            self.conn.execute("MATCH (r:Resource {id: $rid}), (n:SynapsePipeline {id: $nid}) MERGE (r)-[:SynapseContains]->(n)",
                              parameters={"rid": workspace_resource_id, "nid": p["id"]})

        # 2. Notebooks
        for n in artifacts.get("notebooks", []):
            self.conn.execute("MERGE (node:SynapseNotebook {id: $id}) SET node.name = $name, node.workspace = $ws", 
                              parameters={"id": n["id"], "name": n["name"], "ws": n["workspace"]})
            self.conn.execute("MATCH (r:Resource {id: $rid}), (node:SynapseNotebook {id: $nid}) MERGE (r)-[:SynapseContainsNotebook]->(node)",
                              parameters={"rid": workspace_resource_id, "nid": n["id"]})

        # 3. Datasets
        for d in artifacts.get("datasets", []):
            self.conn.execute("MERGE (node:SynapseDataset {id: $id}) SET node.name = $name, node.type = $type, node.workspace = $ws", 
                              parameters={"id": d["id"], "name": d["name"], "type": d["type"], "ws": d["workspace"]})
            self.conn.execute("MATCH (r:Resource {id: $rid}), (node:SynapseDataset {id: $nid}) MERGE (r)-[:SynapseContainsDataset]->(node)",
                              parameters={"rid": workspace_resource_id, "nid": d["id"]})

        # 4. Linked Services
        for ls in artifacts.get("linked_services", []):
            self.conn.execute("MERGE (node:SynapseLinkedService {id: $id}) SET node.name = $name, node.type = $type, node.workspace = $ws", 
                              parameters={"id": ls["id"], "name": ls["name"], "type": ls["type"], "ws": ls["workspace"]})
            self.conn.execute("MATCH (r:Resource {id: $rid}), (node:SynapseLinkedService {id: $nid}) MERGE (r)-[:SynapseContainsLinkedService]->(node)",
                              parameters={"rid": workspace_resource_id, "nid": ls["id"]})

    def upsert_adf_artifacts(self, factory_resource_id, artifacts):
        # 1. Pipelines
        for p in artifacts.get("pipelines", []):
            self.conn.execute("MERGE (n:ADFPipeline {id: $id}) SET n.name = $name, n.factory = $fac", 
                              parameters={"id": p["id"], "name": p["name"], "fac": p["factory_name"]})
            self.conn.execute("MATCH (r:Resource {id: $rid}), (n:ADFPipeline {id: $nid}) MERGE (r)-[:ADFContainsPipeline]->(n)",
                              parameters={"rid": factory_resource_id, "nid": p["id"]})

        # 2. Datasets
        for d in artifacts.get("datasets", []):
            self.conn.execute("MERGE (n:ADFDataset {id: $id}) SET n.name = $name, n.type = $type, n.factory = $fac", 
                              parameters={"id": d["id"], "name": d["name"], "type": d["type"], "fac": d["factory_name"]})
            self.conn.execute("MATCH (r:Resource {id: $rid}), (n:ADFDataset {id: $nid}) MERGE (r)-[:ADFContainsDataset]->(n)",
                              parameters={"rid": factory_resource_id, "nid": d["id"]})

        # 3. Linked Services
        for ls in artifacts.get("linked_services", []):
            self.conn.execute("MERGE (n:ADFLinkedService {id: $id}) SET n.name = $name, n.type = $type, n.factory = $fac", 
                              parameters={"id": ls["id"], "name": ls["name"], "type": ls["type"], "fac": ls["factory_name"]})
            self.conn.execute("MATCH (r:Resource {id: $rid}), (n:ADFLinkedService {id: $nid}) MERGE (r)-[:ADFContainsLinkedService]->(n)",
                              parameters={"rid": factory_resource_id, "nid": ls["id"]})

    def get_graph_stats(self):
        users = self.conn.execute("MATCH (u:User) RETURN count(u)").get_next()[0]
        resources = self.conn.execute("MATCH (r:Resource) RETURN count(r)").get_next()[0]
        return {"users": users, "resources": resources}
