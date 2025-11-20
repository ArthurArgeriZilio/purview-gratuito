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
from src.database.db_manager import DBManager

st.set_page_config(page_title="OpenPurview Local", layout="wide")

st.title("OpenPurview Local 🛡️")
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

# --- Dashboard ---

tab1, tab2, tab3 = st.tabs(["📊 Overview", "🔎 Asset Search", "🕸️ Graph Explorer"])

with tab1:
    st.subheader("Inventory Stats")
    stats = db.get_graph_stats()
    col1, col2 = st.columns(2)
    col1.metric("Total Users", stats["users"])
    col2.metric("Total Azure Resources", stats["resources"])

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
