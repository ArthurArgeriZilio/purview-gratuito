import streamlit as st
import pandas as pd
import sys
import os
import asyncio
import streamlit.components.v1 as components
from pyvis.network import Network
from datetime import datetime

# Add the src directory to the path so we can import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.collectors.azure_collector import AzureCollector
from src.collectors.m365_collector import M365Collector
from src.collectors.powerbi_collector import PowerBICollector
from src.collectors.synapse_collector import SynapseCollector
from src.collectors.adf_collector import ADFCollector
from src.collectors.sql_collector import SQLCollector
from src.database.db_manager import DBManager

st.set_page_config(
    page_title="OpenPurview - Free Data Governance", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: 700;
        color: #0078D4;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .stat-card {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 8px;
        border-left: 4px solid #0078D4;
    }
    .section-divider {
        margin: 2rem 0;
        border-top: 2px solid #e0e0e0;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">OpenPurview</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Free & Local Data Governance Platform for Azure & Microsoft 365</div>', unsafe_allow_html=True)

# Initialize DB
@st.cache_resource
def get_db():
    return DBManager()

db = get_db()

# --- Sidebar Navigation ---
st.sidebar.title("Data Collection")
st.sidebar.markdown("Select which data sources to scan:")

with st.sidebar.expander("Azure Infrastructure", expanded=True):
    scan_azure = st.button("Scan Azure Resources", use_container_width=True, type="primary")
    scan_sql = st.button("Scan SQL Databases (Schema)", use_container_width=True)
    scan_synapse = st.button("Scan Synapse Artifacts", use_container_width=True)
    scan_adf = st.button("Scan Data Factory", use_container_width=True)

with st.sidebar.expander("Microsoft 365", expanded=True):
    scan_m365 = st.button("Scan M365 Users & Groups", use_container_width=True, type="primary")
    
with st.sidebar.expander("Power BI", expanded=True):
    scan_pbi = st.button("Scan Power BI Tenant (Admin)", use_container_width=True, type="primary")

st.sidebar.markdown("---")
st.sidebar.markdown("### System Info")
st.sidebar.caption("Database: KuzuDB (Graph)")
st.sidebar.caption("Authentication: Azure AD")
st.sidebar.info("Ensure you have run `az login` before scanning Azure resources.")

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

if scan_sql:
    st.info("Scanning SQL Databases...")
    try:
        with st.spinner("Querying SQL Databases from inventory..."):
            # Get all SQL databases from the Resource table
            sql_dbs = db.conn.execute("""
                MATCH (r:Resource)
                WHERE r.type = 'microsoft.sql/servers/databases' 
                   OR r.type = 'microsoft.sql/servers'
                RETURN r.id, r.name, r.resourceGroup, r.subscriptionId
            """).get_as_df()
            
            if sql_dbs.empty:
                st.warning("No SQL Databases found. Run Azure Resource scan first.")
            else:
                count = 0
                total_tables = 0
                total_columns = 0
                
                for _, row in sql_dbs.iterrows():
                    db_id = row[0]
                    db_name = row[1]
                    
                    # Extract server name from resource ID
                    # Format: /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Sql/servers/{server}/databases/{db}
                    parts = db_id.split('/')
                    if 'servers' in parts:
                        server_idx = parts.index('servers')
                        server_name = parts[server_idx + 1]
                        
                        st.write(f"Scanning Database: **{server_name}/{db_name}**")
                        
                        # Initialize SQL collector
                        sql_col = SQLCollector()
                        schema = sql_col.scan_database_schema(server_name, db_name)
                        
                        # Store schema in graph database
                        db.upsert_sql_schema(db_id, schema)
                        
                        n_tables = len(schema.get('tables', []))
                        n_cols = sum(len(t.get('columns', [])) for t in schema.get('tables', []))
                        total_tables += n_tables
                        total_columns += n_cols
                        
                        st.caption(f"Found {n_tables} tables, {n_cols} columns.")
                        count += 1
                
                st.success(f"Scanned {count} SQL Databases. Total: {total_tables} tables, {total_columns} columns.")
    
    except Exception as e:
        st.error(f"SQL Scan Failed: {e}")

# --- Dashboard ---

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Overview Dashboard", 
    "Asset Catalog", 
    "Data Lineage", 
    "Compliance & Security",
    "Graph Explorer"
])

with tab1:
    st.header("Data Estate Overview")
    
    # Get statistics
    stats = db.get_graph_stats()
    
    # Top metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Assets",
            value=stats["resources"],
            help="All Azure resources discovered"
        )
    
    with col2:
        st.metric(
            label="Identities",
            value=stats["users"],
            help="Microsoft 365 users and service principals"
        )
    
    with col3:
        # Count Power BI assets
        try:
            pbi_count = db.conn.execute("MATCH (p:PBIReport) RETURN COUNT(p) AS cnt").get_as_df()
            pbi_total = int(pbi_count['cnt'].iloc[0]) if not pbi_count.empty else 0
        except:
            pbi_total = 0
        st.metric(
            label="Power BI Reports",
            value=pbi_total,
            help="Reports discovered across all workspaces"
        )
    
    with col4:
        # Count SQL Tables
        try:
            sql_count = db.conn.execute("MATCH (t:SQLTable) RETURN COUNT(t) AS cnt").get_as_df()
            sql_total = int(sql_count['cnt'].iloc[0]) if not sql_count.empty else 0
        except:
            sql_total = 0
        st.metric(
            label="SQL Tables",
            value=sql_total,
            help="Database tables with full schema"
        )
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    # Resource breakdown
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Resource Distribution by Type")
        try:
            res_by_type = db.conn.execute("""
                MATCH (r:Resource)
                RETURN r.type AS Type, COUNT(r) AS Count
                ORDER BY Count DESC
                LIMIT 10
            """).get_as_df()
            
            if not res_by_type.empty:
                st.bar_chart(res_by_type.set_index('Type'))
            else:
                st.info("No resources scanned yet. Click 'Scan Azure Resources' in the sidebar.")
        except Exception as e:
            st.warning(f"Unable to load resource distribution: {e}")
    
    with col2:
        st.subheader("Resource Distribution by Location")
        try:
            res_by_loc = db.conn.execute("""
                MATCH (r:Resource)
                WHERE r.location IS NOT NULL
                RETURN r.location AS Location, COUNT(r) AS Count
                ORDER BY Count DESC
                LIMIT 10
            """).get_as_df()
            
            if not res_by_loc.empty:
                st.bar_chart(res_by_loc.set_index('Location'))
            else:
                st.info("No location data available.")
        except Exception as e:
            st.warning(f"Unable to load location distribution: {e}")
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    # Recent activity / Quick stats
    st.subheader("Data Estate Health")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        try:
            sub_count = db.conn.execute("MATCH (s:Subscription) RETURN COUNT(s) AS cnt").get_as_df()
            sub_total = int(sub_count['cnt'].iloc[0]) if not sub_count.empty else 0
            st.metric("Subscriptions", sub_total)
        except:
            st.metric("Subscriptions", 0)
    
    with col2:
        try:
            ws_count = db.conn.execute("MATCH (w:PBIWorkspace) RETURN COUNT(w) AS cnt").get_as_df()
            ws_total = int(ws_count['cnt'].iloc[0]) if not ws_count.empty else 0
            st.metric("Power BI Workspaces", ws_total)
        except:
            st.metric("Power BI Workspaces", 0)
    
    with col3:
        try:
            adf_count = db.conn.execute("MATCH (a:ADFPipeline) RETURN COUNT(a) AS cnt").get_as_df()
            adf_total = int(adf_count['cnt'].iloc[0]) if not adf_count.empty else 0
            st.metric("ADF Pipelines", adf_total)
        except:
            st.metric("ADF Pipelines", 0)

with tab2:
    st.header("Asset Catalog")
    st.markdown("Search and browse all discovered assets across your data estate.")
    
    # Search functionality
    col1, col2 = st.columns([3, 1])
    with col1:
        search_term = st.text_input("Search by name, type, or location", placeholder="e.g., storage, sqlserver, eastus")
    with col2:
        asset_type = st.selectbox("Filter by Type", ["All", "User", "Resource", "PBIReport", "PBIDataset", "SQLTable"])
    
    if search_term:
        st.subheader("Search Results")
        
        try:
            # Search across multiple node types
            results_found = False
            
            # Search Users
            if asset_type in ["All", "User"]:
                user_res = db.conn.execute(f"""
                    MATCH (u:User) 
                    WHERE u.displayName CONTAINS '{search_term}' OR u.mail CONTAINS '{search_term}'
                    RETURN 'User' AS Type, u.displayName AS Name, u.mail AS Details
                    LIMIT 20
                """).get_as_df()
                
                if not user_res.empty:
                    st.markdown("**Microsoft 365 Users:**")
                    st.dataframe(user_res, use_container_width=True)
                    results_found = True
            
            # Search Resources
            if asset_type in ["All", "Resource"]:
                res_res = db.conn.execute(f"""
                    MATCH (r:Resource) 
                    WHERE r.name CONTAINS '{search_term}' OR r.type CONTAINS '{search_term}' OR r.location CONTAINS '{search_term}'
                    RETURN 'Resource' AS Type, r.name AS Name, r.type AS ResourceType, r.location AS Location
                    LIMIT 20
                """).get_as_df()
                
                if not res_res.empty:
                    st.markdown("**Azure Resources:**")
                    st.dataframe(res_res, use_container_width=True)
                    results_found = True
            
            # Search Power BI Reports
            if asset_type in ["All", "PBIReport"]:
                pbi_res = db.conn.execute(f"""
                    MATCH (p:PBIReport) 
                    WHERE p.name CONTAINS '{search_term}'
                    RETURN 'Power BI Report' AS Type, p.name AS Name, p.id AS ID
                    LIMIT 20
                """).get_as_df()
                
                if not pbi_res.empty:
                    st.markdown("**Power BI Reports:**")
                    st.dataframe(pbi_res, use_container_width=True)
                    results_found = True
            
            # Search SQL Tables
            if asset_type in ["All", "SQLTable"]:
                sql_res = db.conn.execute(f"""
                    MATCH (t:SQLTable) 
                    WHERE t.name CONTAINS '{search_term}' OR t.schema CONTAINS '{search_term}'
                    RETURN 'SQL Table' AS Type, t.schema AS Schema, t.name AS Name
                    LIMIT 20
                """).get_as_df()
                
                if not sql_res.empty:
                    st.markdown("**SQL Database Tables:**")
                    st.dataframe(sql_res, use_container_width=True)
                    results_found = True
            
            if not results_found:
                st.info("No assets found matching your search criteria.")
                
        except Exception as e:
            st.error(f"Search error: {e}")
    else:
        # Show recent assets
        st.subheader("Recently Discovered Assets")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Latest Azure Resources:**")
            try:
                recent_res = db.conn.execute("""
                    MATCH (r:Resource)
                    RETURN r.name AS Name, r.type AS Type, r.location AS Location
                    LIMIT 10
                """).get_as_df()
                
                if not recent_res.empty:
                    st.dataframe(recent_res, use_container_width=True)
                else:
                    st.info("No resources discovered yet.")
            except Exception as e:
                st.warning(f"Unable to load resources: {e}")
        
        with col2:
            st.markdown("**Latest Power BI Reports:**")
            try:
                recent_pbi = db.conn.execute("""
                    MATCH (p:PBIReport)
                    RETURN p.name AS Name, p.id AS ID
                    LIMIT 10
                """).get_as_df()
                
                if not recent_pbi.empty:
                    st.dataframe(recent_pbi, use_container_width=True)
                else:
                    st.info("No Power BI reports discovered yet.")
            except Exception as e:
                st.warning(f"Unable to load Power BI reports: {e}")

with tab3:
    st.header("Data Lineage")
    st.markdown("Visualize data flow and dependencies across your data estate.")
    
    lineage_type = st.radio(
        "Select Lineage View:",
        ["Power BI Dataset → Report", "ADF Pipeline → Datasets", "SQL Foreign Keys"],
        horizontal=True
    )
    
    try:
        if lineage_type == "Power BI Dataset → Report":
            st.subheader("Power BI Data Flow")
            pbi_lineage = db.conn.execute("""
                MATCH (d:PBIDataset)-[:PBIFeeds]->(r:PBIReport)
                RETURN d.name AS Dataset, r.name AS Report
                LIMIT 50
            """).get_as_df()
            
            if not pbi_lineage.empty:
                st.dataframe(pbi_lineage, use_container_width=True)
                st.caption(f"Showing {len(pbi_lineage)} dataset-to-report relationships")
            else:
                st.info("No Power BI lineage data available. Run Power BI scan first.")
        
        elif lineage_type == "ADF Pipeline → Datasets":
            st.subheader("Azure Data Factory Lineage")
            adf_lineage = db.conn.execute("""
                MATCH (p:ADFPipeline)<-[:ADFContainsPipeline]-(r:Resource)
                RETURN r.name AS DataFactory, p.name AS Pipeline
                LIMIT 50
            """).get_as_df()
            
            if not adf_lineage.empty:
                st.dataframe(adf_lineage, use_container_width=True)
                st.caption(f"Showing {len(adf_lineage)} pipelines")
            else:
                st.info("No ADF lineage data available. Run ADF scan first.")
        
        else:  # SQL Foreign Keys
            st.subheader("SQL Database Relationships")
            sql_lineage = db.conn.execute("""
                MATCH (t1:SQLTable)-[fk:SQLForeignKey]->(t2:SQLTable)
                RETURN t1.schema AS SourceSchema, t1.name AS SourceTable, 
                       t2.schema AS TargetSchema, t2.name AS TargetTable
                LIMIT 50
            """).get_as_df()
            
            if not sql_lineage.empty:
                st.dataframe(sql_lineage, use_container_width=True)
                st.caption(f"Showing {len(sql_lineage)} foreign key relationships")
            else:
                st.info("No SQL relationships available. Run SQL schema scan first.")
    
    except Exception as e:
        st.warning(f"Unable to load lineage: {e}")

with tab4:
    st.header("Compliance & Security")
    st.markdown("Identify potential security risks and compliance issues.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Resource Access Analysis")
        try:
            # Count resources by subscription
            sub_resources = db.conn.execute("""
                MATCH (s:Subscription)-[:Contains]->(r:Resource)
                RETURN s.name AS Subscription, COUNT(r) AS ResourceCount
                ORDER BY ResourceCount DESC
            """).get_as_df()
            
            if not sub_resources.empty:
                st.dataframe(sub_resources, use_container_width=True)
            else:
                st.info("No subscription data available.")
        except Exception as e:
            st.warning(f"Unable to load access data: {e}")
    
    with col2:
        st.subheader("Power BI Workspace Security")
        try:
            ws_stats = db.conn.execute("""
                MATCH (w:PBIWorkspace)
                RETURN w.name AS Workspace, w.state AS State
                LIMIT 20
            """).get_as_df()
            
            if not ws_stats.empty:
                st.dataframe(ws_stats, use_container_width=True)
            else:
                st.info("No Power BI workspace data available.")
        except Exception as e:
            st.warning(f"Unable to load workspace data: {e}")
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    st.subheader("SQL Database Schema Inspection")
    try:
        # Show tables with column counts
        table_info = db.conn.execute("""
            MATCH (t:SQLTable)
            OPTIONAL MATCH (t)-[:SQLHasColumn]->(c:SQLColumn)
            RETURN t.schema AS Schema, t.name AS TableName, COUNT(c) AS ColumnCount
            ORDER BY ColumnCount DESC
            LIMIT 20
        """).get_as_df()
        
        if not table_info.empty:
            st.dataframe(table_info, use_container_width=True)
            st.caption("Tables ordered by column count - useful for identifying large schemas")
        else:
            st.info("No SQL schema data available. Run SQL schema scan first.")
    except Exception as e:
        st.warning(f"Unable to load SQL schema data: {e}")

with tab5:
    st.header("Interactive Graph Explorer")
    st.markdown("Visualize relationships between assets in your data estate.")
    
    # Graph configuration
    col1, col2, col3 = st.columns(3)
    with col1:
        graph_type = st.selectbox(
            "Select Relationship Type:",
            ["Subscription → Resources", "Power BI Workspace → Reports", "SQL Foreign Keys"]
        )
    with col2:
        node_limit = st.slider("Maximum nodes to display:", 10, 200, 50)
    with col3:
        show_labels = st.checkbox("Show node labels", value=True)
    
    if st.button("Generate Graph Visualization", type="primary"):
        try:
            net = Network(
                height="700px", 
                width="100%", 
                bgcolor="#1a1a1a", 
                font_color="white",
                notebook=False
            )
            
            # Configure physics
            net.barnes_hut(
                gravity=-8000,
                central_gravity=0.3,
                spring_length=200,
                spring_strength=0.001,
                damping=0.09
            )
            
            if graph_type == "Subscription → Resources":
                nodes_df = db.conn.execute(f"""
                    MATCH (s:Subscription)-[:Contains]->(r:Resource)
                    RETURN s.id AS sub_id, s.name AS sub_name, 
                           r.id AS res_id, r.name AS res_name, r.type AS res_type
                    LIMIT {node_limit}
                """).get_as_df()
                
                if not nodes_df.empty:
                    # Add subscription nodes
                    subs_added = set()
                    for _, row in nodes_df.iterrows():
                        sub_id = str(row['sub_id'])
                        if sub_id not in subs_added:
                            net.add_node(
                                sub_id,
                                label=str(row['sub_name']) if show_labels else "",
                                title=f"Subscription: {row['sub_name']}",
                                color="#0078D4",
                                size=30,
                                shape="box"
                            )
                            subs_added.add(sub_id)
                        
                        # Add resource node
                        res_id = str(row['res_id'])
                        net.add_node(
                            res_id,
                            label=str(row['res_name'])[:20] if show_labels else "",
                            title=f"{row['res_type']}\n{row['res_name']}",
                            color="#4CAF50",
                            size=15
                        )
                        
                        # Add edge
                        net.add_edge(sub_id, res_id)
                    
                    st.success(f"Visualizing {len(nodes_df)} resources across subscriptions")
                else:
                    st.warning("No subscription-resource relationships found.")
            
            elif graph_type == "Power BI Workspace → Reports":
                nodes_df = db.conn.execute(f"""
                    MATCH (w:PBIWorkspace)-[:PBIContains]->(r:PBIReport)
                    RETURN w.id AS ws_id, w.name AS ws_name,
                           r.id AS rep_id, r.name AS rep_name
                    LIMIT {node_limit}
                """).get_as_df()
                
                if not nodes_df.empty:
                    ws_added = set()
                    for _, row in nodes_df.iterrows():
                        ws_id = str(row['ws_id'])
                        if ws_id not in ws_added:
                            net.add_node(
                                ws_id,
                                label=str(row['ws_name']) if show_labels else "",
                                title=f"Workspace: {row['ws_name']}",
                                color="#FFB900",
                                size=25,
                                shape="box"
                            )
                            ws_added.add(ws_id)
                        
                        rep_id = str(row['rep_id'])
                        net.add_node(
                            rep_id,
                            label=str(row['rep_name'])[:20] if show_labels else "",
                            title=f"Report: {row['rep_name']}",
                            color="#E81123",
                            size=15
                        )
                        
                        net.add_edge(ws_id, rep_id)
                    
                    st.success(f"Visualizing {len(nodes_df)} Power BI reports")
                else:
                    st.warning("No Power BI workspace-report relationships found.")
            
            else:  # SQL Foreign Keys
                nodes_df = db.conn.execute(f"""
                    MATCH (t1:SQLTable)-[fk:SQLForeignKey]->(t2:SQLTable)
                    RETURN t1.id AS src_id, t1.schema AS src_schema, t1.name AS src_name,
                           t2.id AS tgt_id, t2.schema AS tgt_schema, t2.name AS tgt_name
                    LIMIT {node_limit}
                """).get_as_df()
                
                if not nodes_df.empty:
                    tables_added = set()
                    for _, row in nodes_df.iterrows():
                        src_id = str(row['src_id'])
                        if src_id not in tables_added:
                            net.add_node(
                                src_id,
                                label=f"{row['src_schema']}.{row['src_name']}" if show_labels else "",
                                title=f"Table: {row['src_schema']}.{row['src_name']}",
                                color="#7FBA00",
                                size=20
                            )
                            tables_added.add(src_id)
                        
                        tgt_id = str(row['tgt_id'])
                        if tgt_id not in tables_added:
                            net.add_node(
                                tgt_id,
                                label=f"{row['tgt_schema']}.{row['tgt_name']}" if show_labels else "",
                                title=f"Table: {row['tgt_schema']}.{row['tgt_name']}",
                                color="#7FBA00",
                                size=20
                            )
                            tables_added.add(tgt_id)
                        
                        net.add_edge(src_id, tgt_id, arrows="to")
                    
                    st.success(f"Visualizing {len(nodes_df)} foreign key relationships")
                else:
                    st.warning("No SQL foreign key relationships found.")
            
            # Render graph
            if len(net.nodes) > 0:
                net.save_graph("graph.html")
                with open("graph.html", 'r', encoding='utf-8') as f:
                    source_code = f.read()
                components.html(source_code, height=750)
                
                st.caption("Tip: Click and drag nodes, scroll to zoom, hover for details")
            else:
                st.info("No data available for the selected graph type. Run the appropriate scan first.")
                
        except Exception as e:
            st.error(f"Graph visualization error: {e}")
    
    else:
        st.info("Click 'Generate Graph Visualization' to create an interactive network diagram.")

st.sidebar.markdown("---")
st.sidebar.caption(f"OpenPurview v1.0 | Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
