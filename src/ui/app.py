import streamlit as st
import pandas as pd
import sys
import os
import asyncio
import streamlit.components.v1 as components
from pyvis.network import Network

# Add the src directory to the path so we can import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.collectors.azure_collector import AzureCollector
from src.collectors.m365_collector import M365Collector
from src.collectors.powerbi_collector import PowerBICollector
from src.collectors.synapse_collector import SynapseCollector
from src.collectors.adf_collector import ADFCollector
from src.database.db_manager import DBManager

st.set_page_config(page_title="OpenPurview Local", layout="wide")

st.title("OpenPurview Local")
st.markdown("### Your Local, Free Azure & M365 Governance Tool")

# Initialize DB
@st.cache_resource
def get_db():
    return DBManager()

db = get_db()

# --- Sidebar ---
st.sidebar.header("Controls")
scan_azure = st.sidebar.button("Scan Azure Resources")
scan_m365 = st.sidebar.button("Scan M365 Users")
scan_pbi = st.sidebar.button("Scan Power BI (Admin)")
scan_synapse = st.sidebar.button("Scan Synapse Artifacts")
scan_adf = st.sidebar.button("Scan Data Factory")

# --- Main Logic ---

if scan_azure:
    with st.spinner("Scanning Azure..."):
        try:
            collector = AzureCollector()
            
            # 1. Subscriptions
            subs = collector.get_subscriptions()
            if subs:
                # The result from ARG is a dict with 'data' usually, or a list depending on the SDK version wrapper
                # My wrapper returns response.data which is a list of dicts
                for sub in subs:
                    # Normalize keys if needed. ARG returns 'subscriptionId', 'displayName' usually comes from a different call
                    # But my query was: project subscriptionId, name, ...
                    # So 'name' is the display name.
                    db.upsert_subscription({"subscriptionId": sub.get("subscriptionId"), "displayName": sub.get("name")})
                st.success(f"Synced {len(subs)} subscriptions.")

            # 2. Resources
            resources = collector.get_all_resources()
            if resources:
                count = 0
                for res in resources:
                    db.upsert_resource(res)
                    # Link to subscription
                    if res.get("subscriptionId"):
                        db.link_subscription_resource(res.get("subscriptionId"), res.get("id"))
                    count += 1
                st.success(f"Synced {count} resources.")
            
        except Exception as e:
            st.error(f"Azure Scan Failed: {e}")

if scan_m365:
    with st.spinner("Scanning Microsoft 365..."):
        try:
            m365 = M365Collector()
            # Run async function
            users = asyncio.run(m365.get_all_users())
            if users:
                for user in users:
                    db.upsert_user(user)
                st.success(f"Synced {len(users)} users.")
            else:
                st.warning("No users found or permission denied.")
        except Exception as e:
            st.error(f"M365 Scan Failed: {e}")

if scan_pbi:
    with st.spinner("Scanning Power BI Tenant (Scanner API)..."):
        try:
            pbi = PowerBICollector()
            
            # 1. Get all Workspace IDs
            st.info("Fetching all workspaces...")
            workspaces = pbi.get_all_workspaces()
            ws_ids = [w['id'] for w in workspaces]
            
            if not ws_ids:
                st.warning("No workspaces found or Admin API access denied.")
            else:
                st.info(f"Found {len(ws_ids)} workspaces. Starting deep scan...")
                
                # 2. Run Scanner API
                scan_results = pbi.run_scanner_api(ws_ids)
                
                # 3. Ingest Data
                ws_count = 0
                report_count = 0
                dataset_count = 0
                
                for ws in scan_results:
                    # Upsert Workspace
                    db.upsert_pbi_workspace(ws)
                    ws_count += 1
                    
                    # Upsert Reports
                    for report in ws.get("reports", []):
                        db.upsert_pbi_report(report, ws['id'])
                        report_count += 1
                        # Link Dataset -> Report
                        if report.get("datasetId"):
                            db.link_dataset_to_report(report.get("datasetId"), report.get("id"))
                            
                    # Upsert Datasets
                    for dataset in ws.get("datasets", []):
                        db.upsert_pbi_dataset(dataset, ws['id'])
                        dataset_count += 1
                
                st.success(f"Scan Complete! Synced {ws_count} Workspaces, {report_count} Reports, {dataset_count} Datasets.")
                
        except Exception as e:
            st.error(f"Power BI Scan Failed: {e}")

if scan_synapse:
    with st.spinner("Scanning Synapse Workspaces..."):
        try:
            # 1. Find Synapse Workspaces in DB (populated by Azure Scan)
            # We look for resources with type 'microsoft.synapse/workspaces'
            # Note: Azure Resource Graph returns types in lowercase usually.
            ws_df = db.conn.execute("MATCH (r:Resource) WHERE r.type =~ '(?i)microsoft.synapse/workspaces' RETURN r.id, r.name").get_as_df()
            
            if ws_df.empty:
                st.warning("No Synapse Workspaces found in local DB. Please run 'Scan Azure Resources' first.")
            else:
                synapse_col = SynapseCollector()
                count = 0
                
                for index, row in ws_df.iterrows():
                    ws_name = row[1]
                    ws_id = row[0]
                    
                    st.write(f"Scanning Workspace: **{ws_name}**")
                    artifacts = synapse_col.scan_workspace_artifacts(ws_name)
                    
                    db.upsert_synapse_artifacts(ws_id, artifacts)
                    
                    n_pipelines = len(artifacts['pipelines'])
                    n_notebooks = len(artifacts['notebooks'])
                    st.caption(f"Found {n_pipelines} pipelines, {n_notebooks} notebooks.")
                    count += 1
                
                st.success(f"Scanned {count} Synapse Workspaces.")

        except Exception as e:
            st.error(f"Synapse Scan Failed: {e}")

if scan_adf:
    with st.spinner("Scanning Azure Data Factories..."):
        try:
            # 1. Find ADFs in DB
            adf_df = db.conn.execute("MATCH (r:Resource) WHERE r.type =~ '(?i)microsoft.datafactory/factories' RETURN r.id, r.name, r.resourceGroup, r.subscriptionId").get_as_df()
            
            if adf_df.empty:
                st.warning("No Data Factories found in local DB. Please run 'Scan Azure Resources' first.")
            else:
                count = 0
                for index, row in adf_df.iterrows():
                    adf_id = row[0]
                    adf_name = row[1]
                    rg_name = row[2]
                    sub_id = row[3]
                    
                    st.write(f"Scanning ADF: **{adf_name}**")
                    
                    # Initialize collector with specific subscription
                    adf_col = ADFCollector(sub_id)
                    artifacts = adf_col.scan_adf(rg_name, adf_name)
                    
                    db.upsert_adf_artifacts(adf_id, artifacts)
                    
                    n_pipes = len(artifacts['pipelines'])
                    n_ds = len(artifacts['datasets'])
                    st.caption(f"Found {n_pipes} pipelines, {n_ds} datasets.")
                    count += 1
                
                st.success(f"Scanned {count} Data Factories.")

        except Exception as e:
            st.error(f"ADF Scan Failed: {e}")

# --- Dashboard ---

tab1, tab2, tab3 = st.tabs(["Overview", "Asset Search", "Graph Explorer"])

with tab1:
    st.subheader("Inventory Stats")
    stats = db.get_graph_stats()
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Users", stats["users"])
    col2.metric("Total Azure Resources", stats["resources"])
    # Add PBI stats if available in get_graph_stats, or just leave as is for now
    # Ideally update get_graph_stats to return PBI counts too


with tab2:
    st.subheader("Search Database")
    search_term = st.text_input("Search for a User or Resource Name")
    if search_term:
        # Simple Cypher search
        try:
            # Search Users
            user_res = db.conn.execute(f"MATCH (u:User) WHERE u.displayName CONTAINS '{search_term}' RETURN u.displayName, u.mail").get_as_df()
            if not user_res.empty:
                st.write("Users found:")
                st.dataframe(user_res)
            
            # Search Resources
            res_res = db.conn.execute(f"MATCH (r:Resource) WHERE r.name CONTAINS '{search_term}' RETURN r.name, r.type, r.location").get_as_df()
            if not res_res.empty:
                st.write("Resources found:")
                st.dataframe(res_res)
        except Exception as e:
            st.error(f"Search error: {e}")

with tab3:
    st.subheader("Relationship Visualizer")
    st.info("Visualizing the first 50 nodes and their connections.")
    
    # Generate PyVis Graph
    try:
        net = Network(height="600px", width="100%", bgcolor="#222222", font_color="white")
        
        # Fetch some data
        # Get Subscriptions and Resources
        nodes = db.conn.execute("MATCH (s:Subscription)-[:Contains]->(r:Resource) RETURN s.name, r.name, r.type LIMIT 50").get_as_df()
        
        if not nodes.empty:
            for index, row in nodes.iterrows():
                sub_name = row[0]
                res_name = row[1]
                res_type = row[2]
                
                net.add_node(sub_name, label=sub_name, title="Subscription", color="#0078D4") # Azure Blue
                net.add_node(res_name, label=res_name, title=res_type, color="#4CAF50") # Green
                net.add_edge(sub_name, res_name)
        
        # Save and read
        net.save_graph("graph.html")
        with open("graph.html", 'r', encoding='utf-8') as f:
            source_code = f.read()
        components.html(source_code, height=600)
        
    except Exception as e:
        st.warning(f"Graph visualization error (DB might be empty): {e}")

st.sidebar.markdown("---")
st.sidebar.info("Run `az login` in your terminal before starting the scan.")
