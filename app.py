import streamlit as st
import pandas as pd
import duckdb
import os
from src.agent import app as agent_app

st.set_page_config(
    page_title="Executive Data AI", 
    page_icon="📊",
    layout="wide"
    )
# st.title("Executive Data Assistant")
DB_PATH = st.secrets["passwords"]["DB_PATH"]

def login_screen():
    """Returns True if the user is authenticated, False otherwise."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if st.session_state.authenticated:
        return True
    
    # Login UI
    st.container()
    with st.columns([1, 2, 1])[1]: #center the login box
        st.title("Executive Data Assistant")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if st.button("Login", width='stretch'):
            # Check against secrets.toml
            if username in st.secrets["passwords"] and password == st.secrets["passwords"][username]:
                st.session_state.authenticated = True
                st.session_state.user = username
                st.success(f"Welcome back, {username.upper()}!")
                st.rerun()
            else:
                st.error("Invalid username or passworrd.")
    return False


# if not login_screen():
#     st.stop()

# Initialize Database COnnection
def get_db_con():
    return duckdb.connect(DB_PATH)

def get_db_con_ro():
    return duckdb.connect(DB_PATH, read_only=True)

def get_all_tables():
    """Queries DuckDB to find all user-created tables."""
    with get_db_con() as con:
        tables = con.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
            """).fetchall()
    return [t[0] for t in tables]

@st.fragment
def data_ingestion_ui():
    st.header("Data Management")

    with st.form("upload_form", clear_on_submit=True):
        uploaded_files = st.file_uploader(
            "Upload Excel Files",
            type=["xlsx", "xlsb"],
            accept_multiple_files=True
        )
        submit = st.form_submit_button("Create Table", use_container_width=True)
    
    if submit and uploaded_files:
        for uploaded_file in uploaded_files:
            base_name = uploaded_file.name.lower()
            for ext in [".xlsx", ".xlsb"]:
                base_name = base_name.replace(ext, "")
            table_name = base_name.replace(" ","_").strip()

            with st.status(f"Ingesting {table_name} ...", expanded=True) as status:
                try:
                    df = pd.read_excel(uploaded_file, engine='calamine', dtype=str)
                    with get_db_con() as con:
                        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
                    status.update(label=f"✅ {table_name} Loaded", state="complete")
                    st.toast(f"Table {table_name} created.")
                except Exception as e:
                    st.error(f"Faled to load {table_name}: {e}")
        st.rerun()

def create_master_report_view():
    sql = """
    CREATE OR REPLACE VIEW dashboard AS
    WITH 
    -- 1. Aggregate Employees into Project-Grade buckets
    util_summarized AS (
        SELECT 
            "Project Id", 
            Practice, 
            "Utilization Location" AS Location, 
            "Grade Name" AS Grade,
            ANY_VALUE("Project Name") AS project_name, -- Just grab the name once
            COUNT("Associate ID") AS headcount,
            SUM(TRY_CAST("Billed FTE Internal" AS DOUBLE)) AS total_billed_fte,
            SUM(TRY_CAST("Total FTE" AS DOUBLE)) AS total_fte,
            ANY_VALUE("BU") AS bu_id,
            ANY_VALUE("Customer Id") AS account_id
        FROM utilization_prediction_report
        GROUP BY 1, 2, 3, 4
    ),
    -- 2. Your Demand Summary (already correct)
    demand_summary AS (
        SELECT "Project Id", Practice, Location, "Grade HR" AS Grade, COUNT(*) AS dem_count
        FROM demand_base
        GROUP BY 1, 2, 3, 4
    ),
    -- 3. Your Release Summary (already correct)
    release_summary AS (
        SELECT "Project Id", Practice, Location, Grade, COUNT(*) AS rel_count
        FROM np_jan26
        GROUP BY 1, 2, 3, 4
    )

    -- 4. Final Join (1-to-1-to-1 Join)
    SELECT 
        u."Project Id", u.project_name, u.Practice, u.Location, u.bu_id, u.Grade, u.account_id, u.total_billed_fte,
        COALESCE(r.rel_count, 0) AS release_count,
        COALESCE(d.dem_count, 0) AS open_demands,
        l_map.Country, l_map.Geo,
        b_map.SBU, b_map.Market,
        s_map."SBU Head ID", s_map."SBU Head Name"
    FROM util_summarized u
    LEFT JOIN release_summary r 
        ON  u."Project Id" = r."Project Id" 
        AND u.Practice = r.Practice 
        AND u.Location = r.Location 
        AND u.Grade = r.Grade
    LEFT JOIN demand_summary d 
        ON  u."Project Id" = d."Project Id" 
        AND u.Practice = d.Practice 
        AND u.Location = d.Location 
        AND u.Grade = d.Grade
    LEFT JOIN map_location l_map ON u.Location = l_map."Utilization Location"
    LEFT JOIN map_bu b_map       ON u.bu_id = b_map.BU
    LEFT JOIN map_sbu s_map      ON b_map.SBU = s_map.SBU;
    """

    try:
        with get_db_con() as con:
            con.execute(sql)
        st.success("🚀 Master Project Report Generated!")
        st.toast("AI Context Updated with  Project-level insights.")
        st.rerun()

    except Exception as e:
        st.error(f"Failed to generate report: {e}")

def render_table_group(table_list, key_prefix):
    selected = []
    if not table_list:
        st.caption("No tables found in this category.")
        return selected

    for table in table_list:
        cols = st.columns([0.6, 0.2, 0.2])

        # 1. Selection Checkbox
        if cols[0].checkbox(table, key=f"sel_{key_prefix}_{table}"):
            selected.append(table)
        # 2. Preview
        if cols[1].button("👁️", key=f"pre_{key_prefix}_{table}", help="Preview Data"):
            st.session_state.preview_table = table
            st.rerun()
        # 3. Delete
        if cols[2].button("🗑️", key=f"del_{key_prefix}_{table}", help="Delete table"):
            st.session_state.confirm_delete = table
            st.rerun()

    return selected

@st.dialog("Confirm Deletion")
def confirm_delete_dialog(table_name):
    st.warning(f"Are you sure you want to permanently delete **{table_name}**?")
    c1, c2 = st.columns(2)

    if c1.button("Yes, Delete", type="primary", use_container_width=True):
        try:
            with get_db_con() as con:
                con.execute(f'DROP VIEW IF EXISTS "{table_name}"')
                con.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            del st.session_state.confirm_delete
            st.toast(f"Removed {table_name}")
            st.rerun()

        except Exception as e:
            st.error(f"Error during deletion: {e}")

    if c2.button("Cancel", use_container_width=True):
        del st.session_state.confirm_delete
        st.rerun()

@st.dialog("Table Preview", width="large")
def preview_table_dialog(table_name):
    st.write(f"Showing the first 50 rows of **{table_name}**")

    try:
        with get_db_con_ro() as con:
            df_preview = con.execute(f'SELECT * FROM "{table_name}" LIMIT 50').df()
        
        if not df_preview.empty:
            st.dataframe(df_preview, use_container_width=True, hide_index=True)
        else:
            st.info("This table is currently empty.")
    except Exception as e:
        st.info(f"Could not load preview: {e}")
    
    if st.button("Close Preview", use_container_width=True):
        #del st.session_state.preview_table
        st.rerun()

# The new Sidebar design
with st.sidebar:
    #st.image("util/media/logo.jpg", use_container_width=True)
    st.markdown("#### OPS-ASSIST")
    st.title("📂 Data Management")

    # --- ZONE 1: INGESTION ---
    with st.expander("📤 Upload New Data", expanded=False):
        data_ingestion_ui()
    
    st.divider()

    # --- SONE 2: TABLE MANAGEMENT ---
    st.subheader("🗄️ Active Tables")
    all_tables = get_all_tables()

    # Categorize tables based on namming conventions
    transactions = [t for t in all_tables if not t.startswith(('map_', '_ref', 'v_','dashboard'))]
    lookups = [t  for t in all_tables if t.startswith('map_')]
    views = [t for t in all_tables if t.startswith('v_') or t == "dashboard"]

    # 2a. Transactional Data
    with st.expander("📊 Transactional Tables", expanded=False):
        selected_transactions = render_table_group(transactions, "trans")

    #2b. Lookup / Master Data
    with st.expander("🔍 Lookups & Mappings", expanded=False):
        selected_lookups = render_table_group(lookups, "look")

    # 2c. Final Reports (Views)
    with st.expander("🚀 Analytics Views", expanded=False):
        selected_views = render_table_group(views, "view")

    # Final combined list for AI context
    selected_tables = selected_transactions + selected_lookups + selected_views

    st.divider()

    # --- ZONE 3: ACTIONS ---
    if st.button("🪄 Build Master Report", type="primary", use_container_width=True):
        create_master_report_view()

    st.divider()
    with st.expander("🛠️ SQL Console"):
        query = st.text_area("Paste your Query here:", height=150)
        if st.button("Run Query"):
            with get_db_con_ro() as con:
                res = con.execute(query).df()
                st.dataframe(res)

if "confirm_delete" in st.session_state:
    confirm_delete_dialog(st.session_state.confirm_delete)

if "preview_table" in st.session_state and st.session_state.preview_table:
    table_to_show = st.session_state.preview_table
    st.session_state.preview_table = None
    preview_table_dialog(table_to_show)
    

# Sidebar for setup
# with st.sidebar:
#     st.title("📂 Data Warehouse")
#     data_ingestion_ui()

#     view_name = "dashboard"
 
#     sql_view="""
#             SELECT 
#             t1."Project Id", 
#             t1.Practice, 
#             t1."Utilization Location" as Location, 
#             t1."Grade Name" as Grade, 
#             t1."Billed FTE Internal", 
#             t1."Total FTE",
#             COALESCE(demands.demand_count, 0) AS Demand,
#             attr.attrition
#         FROM utilization_prediction_report t1 
#         LEFT JOIN (
#             SELECT 
#                 "Project Id", 
#                 Practice, 
#                 Location, 
#                 "Grade HR", 
#                 COUNT(*) AS demand_count
#             FROM demand_base
#             GROUP BY "Project Id", Practice, Location, "Grade HR"
#         ) demands 
#         ON  t1."Project Id" = demands."Project Id" 
#         AND t1.Practice = demands.Practice 
#         AND t1."Utilization Location" = demands.Location 
#         AND t1."Grade Name" = demands."Grade HR" 
#         LEFT JOIN (
#             SELECT 
#                 "Project Id", Practice, Location, Grade, 
#                 MAX("Count of ID") as attrition
#             FROM np_jan26
#             GROUP BY 1, 2, 3, 4
#         ) attr 
#             ON  t1."Project Id" = attr."Project Id" 
#             AND t1.Practice = attr.Practice 
#             AND t1."Utilization Location" = attr.Location 
#             AND t1."Grade Name" = attr.Grade;
#         """
#     if st.button("Generate Report", type="primary", use_container_width=True):
#         try:
#             with get_db_con() as con:
#                 con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {sql_view}")
#             st.success(f"View '{view_name}' created!")
#             st.rerun()
#         except Exception as e:
#             st.error(f"SQL Error: {e}")
        

#     # Checkbox selection
#     st.write("---")
#     st.subheader("Select Tables")
#     all_tables = get_all_tables()
#     selected_tables = []

#     if all_tables:
#         for table in all_tables:
#             col1, col2 = st.columns([0.8, 0.2])

#             if col1.checkbox(f"{table}", value=False, key=f"chk_{table}"):
#                 selected_tables.append(table)

#             # Delete button on the right
#             # if col2.button("🗑️", key=f"del_{table}", help=f"Delete {table} permanently", use_container_width=True):
#             #     st.session_state.confirm_delete = table
#             #     st.rerun()
#             if col2.button("❌", key=f"del_{table}", type="secondary"):
#                 st.session_state.confirm_delete = table
#                 st.rerun()
#         if "confirm_delete" in st.session_state:
#             target = st.session_state.confirm_delete
#             st.error(f"Are you sure you want to delete **{target}**?")
#             c1, c2 = st.columns(2)
#             if c1.button("Yes, Delete", type="primary", width='stretch'):
#                 with get_db_con() as con:
#                     con.execute(f'DROP TABLE "{target}"')
#                 del st.session_state.confirm_delete
#                 st.toast(f"Table {target} removed.")
#                 st.rerun()
#             if c2.button("cancel", width='stretch'):
#                 del st.session_state.confirm_delete
#                 st.rerun
#     else:
#         st.info("No files exist. Upload a file to begin.")
    
#     # Data Preview
#     if selected_tables:
#         with st.expander("Table Previews", expanded=False):
#             with get_db_con() as con:
#                 for table in selected_tables:
#                     st.write(f"**{table}** (Top 5 rows)")
#                     preview_df = con.execute(f"SELECT * FROM {table} LIMIT 1000").df()
#                     st.dataframe(preview_df, width='stretch')
#     if not selected_tables and all_tables:
#         st.warning("Select tables to provide context to AI")

# Chat interface
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Handle Input
if prompt := st.chat_input("Ask a question about the selected data ..."):
    if not selected_tables:
        st.error("No data selected in the sidebar!")
    else:
        # Show user message
        st.session_state.messages.append({"role":"user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
    # Run the agent
    # if not os.path.exists("data/data1.xlsx"):
    #     st.error("Please upload as excel file first")
    # else:
        with st.chat_message("assistant"):
            status_container = st.status("Analyzing across files...", expanded=False)

            # Pass the list of selected tables to the agent
            inputs = {
                "question": prompt,
                "active_tables": selected_tables
            }
            

            try:
                # Stream the graph updates
                final_response = ""
                for step in agent_app.stream(inputs):
                    if "generate_query" in step:
                        status_container.write(f" User Question : {prompt}")
                        status_container.write(f" Generated SQL for : `{step['generate_query']['sql_query']}`")
                    # if "execute_query" in step:
                    #     status_container.write("Executed Query in DuckDB")
                    if "summerize" in step:
                        final_response = step['summerize']['messages'][0]
                
                status_container.update(label="Analysis Complete !", state="complete", expanded=False)
                st.markdown(final_response)

                st.session_state.messages.append({"role":"assistant", "content": final_response})
            
            except Exception as e:
                st.error(f"An error occurred: {e}")