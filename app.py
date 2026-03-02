import streamlit as st
import pandas as pd
import duckdb
import os
import io
import datetime
from src.agent import app as agent_app

# from phoenix.otel import register

# if "phoenix_session" not in st.session_state:
#     px.launch_app(host="0.0.0.0", port=6006)
#     register(
#         project_name="ops-assist",
#         endpoint="http://localhost:6006/v1/traces",
#         auto_instrument=True
#     )
#     st.session_state.phoenix_session = True

import phoenix as px
from openinference.instrumentation.langchain import LangChainInstrumentor
from phoenix.otel import register

if "phoenix_session" not in st.session_state:
    st.session_state.phoenix_session = px.launch_app()
    tracer_provider=register(project_name="ops-assist")
    LangChainInstrumentor(tracer_provider=tracer_provider).instrument(skip_dep_check=True)
    

#os.environ["PHOENIX_COLLECTOR_ENDPOINT"] = "http://127.0.0.1:6006"
#LangChainInstrumentor().instrument(skip_if_installed=True)

st.set_page_config(
    page_title="Executive Data AI", 
    page_icon="📊",
    layout="wide"
    )

db_path = os.getenv("DATABASE_PATH")
if not db_path:
    db_path = st.secrets["passwords"]["DB_PATH"]

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
#@st.cache_resource
def get_db_con():
    return duckdb.connect(db_path, read_only=False)

def get_db_con_ro():
    return duckdb.connect(db_path, read_only=True)

# def init_history_db():
#     with get_db_con() as con: # Assuming this is your connection util
#         con.execute("CREATE SEQUENCE IF NOT EXISTS seq_chat_id START 1")
#         con.execute("""
#             CREATE TABLE IF NOT EXISTS chat_history (
#                 id INTEGER PRIMARY KEY DEFAULT nextval('seq_chat_id'),
#                 question TEXT,
#                 timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#             )
#         """)

# def save_question(question):
#     with get_db_con() as con:
#         con.execute("INSERT INTO chat_history (question) SELECT (?) WHERE NOT EXISTS (SELECT 1 FROM chat_history WHERE question = ?)", [question, question])

# def get_recent_questions():
#     with get_db_con_ro() as con:
#         # Get last 10 unique questions
#         df = con.execute("SELECT DISTINCT question FROM chat_history ORDER BY timestamp DESC LIMIT 10").df()
#         return df['question'].tolist()
    
# if "db_initialized" not in st.session_state:
#     init_history_db()
#     st.session_state.db_initialized = True

def get_all_tables():
    """Queries DuckDB to find all user-created tables."""
    with get_db_con_ro() as con:
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
        submit = st.form_submit_button("Create Table", width="stretch")
    
    if submit and uploaded_files:
        for uploaded_file in uploaded_files:
            base_name = uploaded_file.name.lower()
            for ext in [".xlsx", ".xlsb"]:
                base_name = base_name.replace(ext, "")
            table_name = base_name.replace(" ","_").strip()

            with st.status(f"Ingesting {table_name} ...", expanded=True) as status:
                try:
                    df = pd.read_excel(uploaded_file, engine='calamine', dtype=str)
                    df['load_date'] = pd.Timestamp.now(tz='Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S %Z')
                    with get_db_con() as con:
                        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
                    status.update(label=f"✅ {table_name} Loaded", state="complete")
                    st.toast(f"Table {table_name} created.")
                except Exception as e:
                    st.error(f"Faled to load {table_name}: {e}")
        st.rerun()

def create_master_report_view(f_weight, d_weight):
    sql = f"""
        CREATE OR REPLACE VIEW dashboard15 AS
            WITH 
            dept_map AS(
            SELECT "HCM Department Name" AS department_name, ANY_VALUE("HCM Department ID") AS department_id
            from utilization_prediction_report
            where department_name IS NOT NULL
            GROUP BY 1
            ),
            -- 1. Current Month Aggregation
            util_summarized AS (
                SELECT 
                    "Project Id", Practice, "Utilization Location" AS Location, "Grade Name" AS Grade, 
                    "HCM Department Name" AS department_name,
                    COUNT("Associate ID") AS headcount,
                    SUM(TRY_CAST("Billed FTE Internal" AS DOUBLE)) AS billed_fte,
                    SUM(TRY_CAST("Total FTE" AS DOUBLE)) AS total_fte,
                    ANY_VALUE("BU") AS bu_id,
                    ANY_VALUE("Customer Id") AS account_id,
                    ANY_VALUE("Project Name") AS project_name,
                    ANY_VALUE("Project Type") AS project_type,
                    ANY_VALUE("Project Billability") AS project_billability,
                    ANY_VALUE("Customer Name") AS customer_name,
                    ANY_VALUE("ParentCustomerID") AS parent_customer_id,
                    ANY_VALUE("Parent Customer") AS parent_customer,
                    ANY_VALUE("Is Onsite") AS is_onsite
                FROM utilization_prediction_report
                GROUP BY 1, 2, 3, 4, 5
            ),
            -- 2. Previous Month Aggregation (Now with Descriptive attributes)
            previous_util AS (
                SELECT 
                    "Project Id", Practice, "Utilization Location" AS Location, "Grade Name" AS Grade,
                    "HCM Department Name" AS department_name,
                    SUM(TRY_CAST("Billed FTE Internal" AS DOUBLE)) AS prev_mon_billed_fte,
                    SUM(TRY_CAST("Total FTE" AS DOUBLE)) AS prev_mon_total_fte,
                    ANY_VALUE("BU") AS bu_id,
                    ANY_VALUE("Customer Id") AS account_id,
                    ANY_VALUE("Project Name") AS project_name,
                    ANY_VALUE("Project Type") AS project_type,
                    ANY_VALUE("Project Billability") AS project_billability,
                    ANY_VALUE("Customer Name") AS customer_name,
                    ANY_VALUE("ParentCustomerID") AS parent_customer_id,
                    ANY_VALUE("Parent Customer") AS parent_customer
                FROM previous_month_actual
                GROUP BY 1, 2, 3, 4, 5
            ),
            -- 2. Your Demand Summary (already correct)
            demand_summary AS (
                SELECT "Project Id", Practice, Location, "Grade HR" AS Grade, 
                "Pool Name" AS department_name,
                COUNT(*) AS dem_count,
                ANY_VALUE("BU") AS bu_id,
                ANY_VALUE("Account Id") AS account_id,
                ANY_VALUE("Project Description") AS project_name,
                ANY_VALUE("Project Type") AS project_type,
                ANY_VALUE("Project Billability") AS project_billability,
                ANY_VALUE("Account Name") AS customer_name,
                ANY_VALUE("Account ID") AS customer_id,
                ANY_VALUE("Parent Customer ID") AS parent_customer_id,
                ANY_VALUE("Parent Customer") AS parent_customer
                FROM demand_base
                GROUP BY 1, 2, 3, 4, 5
            ),
            fulfilment_summary AS (
                SELECT "Project Id", Practice, Location, "Associate Hired Grade" AS Grade, 
                "Pool Name" AS department_name,
                SUM(TRY_CAST("FTE Impact" AS DOUBLE)) AS fulfil_count
                FROM fulfilment
                GROUP BY 1, 2, 3, 4, 5
            ),

            release_summary AS (
            SELECT "Project Id", Practice, Location, Grade, "Department Name" AS department_name,
                SUM(TRY_CAST("Impact FTE" AS DOUBLE)) AS rel_count
                FROM releases
                GROUP BY 1, 2, 3, 4, 5
            ),
            attrition_summary AS (
            SELECT "Project Id", Practice, Location, Grade, "Department Name" AS department_name,
                SUM(TRY_CAST("FTE Impact" AS DOUBLE)) AS attr_count
                FROM attrition
                GROUP BY 1, 2, 3, 4, 5
            ),
            -- New Cleaned Up Account Map
            map_account_unique AS (
            SELECT "Account ID", ANY_VALUE("PDL ID") AS "PDL ID", ANY_VALUE("PDL Name") AS "PDL Name"
            FROM map_account
            GROUP BY "Account ID"
            ),

            -- 3. Master Keys
            master_keys AS (
                SELECT "Project Id", Practice, Location, Grade, department_name FROM util_summarized
                UNION 
                SELECT "Project Id", Practice, Location, Grade, department_name FROM previous_util
                UNION
                SELECT "Project Id", practice, Location, Grade, department_name From demand_summary
            )

            -- 4. Final Join
            SELECT  
                m."Project Id", 
                COALESCE(u.project_name, p.project_name, d.project_name) AS project_name,
                COALESCE(u.project_type, p.project_type, d.project_type) AS project_type,
                COALESCE(u.project_billability, p.project_billability, d.project_billability) AS project_billability,
                m.Practice, m.Location, m.Grade,
                -- Pull from Current, fallback to Previous
                
                COALESCE(u.account_id, p.account_id, d.account_id) AS customer_id,
                COALESCE(u.customer_name, p.customer_name, d.customer_name) AS customer_name,
                COALESCE(u.parent_customer_id, p.parent_customer_id, d.parent_customer_id) AS parent_customer_id,
                COALESCE(u.parent_customer, p.parent_customer, d.parent_customer) AS parent_customer_name,
                COALESCE(u.bu_id, p.bu_id, d.bu_id) AS BU,
                m.department_name, d_map.department_id, t_map.Tower,
                
                --ROUND(COALESCE(u.billed_fte, 0), 5) AS billed_fte, 
                COALESCE(r.rel_count, 0) AS release_count,
                COALESCE(a.attr_count, 0) AS attr_count,
                COALESCE(d.dem_count, 0) AS open_demands, 
                COALESCE(f.fulfil_count, 0) AS demands_fulfilled,
                
                -- ALL computed columns
                ROUND(CAST(c.Cost AS DOUBLE), 5) AS std_cost,
                
                ROUND(COALESCE(p.prev_mon_billed_fte, 0), 5) AS prev_mon_billed_fte,
                ROUND(COALESCE(p.prev_mon_total_fte, 0), 5) AS prev_mon_total_fte,
                
                ROUND((COALESCE(p.prev_mon_billed_fte, 0) + COALESCE(0.5 * demands_fulfilled, 0) + COALESCE(0.2 *open_demands, 0) - COALESCE(attr_count, 0) - COALESCE(release_count, 0) ), 5) AS eff_billed_fte,
                ROUND((COALESCE(p.prev_mon_total_fte, 0) + COALESCE( demands_fulfilled, 0) + COALESCE(0.5 * open_demands, 0) - COALESCE(attr_count, 0) - COALESCE(release_count, 0) ), 5) AS eff_total_fte,
                
                ROUND((COALESCE(p.prev_mon_billed_fte, 0) * std_cost), 5) AS prev_billed_cost,
                ROUND((COALESCE(p.prev_mon_total_fte, 0) * std_cost), 5) AS prev_mon_total_cost,
                
                ROUND((eff_billed_fte * std_cost), 5) AS proj_billed_cost,
                ROUND((eff_total_fte * std_cost), 5) AS proj_total_cost,
                
                l_map.Country, l_map.Geo,
                b_map.SBU, b_map.Market,
                a_map."PDL ID",
                a_map."PDL Name",
                s_map."SBU Head ID",
                s_map."SBU Head Name"
                
            FROM master_keys m
            LEFT JOIN dept_map d_map ON m.department_name = d_map.department_name
            LEFT JOIN map_tower t_map ON d_map.department_id = t_map."Department ID"
            LEFT JOIN util_summarized u ON m."Project Id" = u."Project Id" AND m.Practice = u.Practice AND m.Location = u.Location AND m.Grade = u.Grade AND m.department_name = u.department_name
            LEFT JOIN previous_util p ON m."Project Id" = p."Project Id" AND m.Practice = p.Practice AND m.Location = p.Location AND m.Grade = p.Grade AND m.department_name = p.department_name
            LEFT JOIN demand_summary d ON m."Project Id" = d."Project Id" AND m.Practice = d.Practice AND m.Location = d.Location AND m.Grade = d.Grade AND m.department_name = d.department_name
            -- IMPORTANT: Join map tables to m (master) or COALESCE values to ensure they work for closed projects
            LEFT JOIN map_location l_map ON m.Location = l_map."Utilization Location"
            LEFT JOIN cost_file c ON m.Practice = c.Practice AND l_map.Country = c.Country AND m.Grade = c."Grade name"
            LEFT JOIN map_bu b_map ON COALESCE(u.bu_id, p.bu_id, d.bu_id) = b_map.BU
            LEFT JOIN map_sbu s_map ON b_map.SBU = s_map.SBU
            LEFT JOIN map_account_unique a_map ON COALESCE(u.account_id, p.account_id) = a_map."Account ID"
            -- Demand and Release still join to m/u/p logic
            LEFT JOIN fulfilment_summary f ON m."Project Id" = f."Project Id" AND m.Practice = f.Practice AND m.Location = f.Location AND m.Grade = f.Grade AND m.department_name = f.department_name
            LEFT JOIN attrition_summary a ON m."Project Id" = a."Project Id" AND m.Practice = a.Practice AND m.Location = a.Location AND m.Grade = a.Grade AND m.department_name = a.department_name
            LEFT JOIN release_summary r ON m."Project Id" = r."Project Id" AND m.Practice = r.Practice AND m.Location = r.Location AND m.Grade = r.Grade AND m.department_name = r.department_name;

    """

    try:
        with get_db_con() as con:
            con.execute(sql)
        st.success("🚀 Master Project Report Generated!")
        st.toast("AI Context Updated with  Project-level insights.")
        st.rerun()

    except Exception as e:
        st.error(f"Failed to generate report: {e}")
def create_pdl_summary_view():
    """
    Aggregates the Dashboard into PDL-level summary.
    """
    sql ="""
        CREATE OR REPLACE VIEW pdl_summary AS
            SELECT 
                --SBU,
                --"SBU Head Name",
                "PDL Name",
                parent_customer_name,
                ANY_VALUE(Practice) as practice, 
                ANY_VALUE(project_type) as project_type, 
                ANY_VALUE(grade) as grade,
                -- Totals
                ROUND(SUM(prev_mon_actual_billed_fte)) AS last_bfte,
                ROUND(SUM(prev_total_cost)/NULLIF(last_bfte,0)) AS prev_cost_per_bfte,
                ROUND(SUM(proj_total_cost)/NULLIF(last_bfte,0)) AS projected_cost_per_bfte,
                (projected_cost_per_bfte - prev_cost_per_bfte) AS diff,
                strftime(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata', '%Y-%m-%d %H:%M:%S') as load_date
            FROM dashboard
            GROUP BY 1, 2
            ORDER BY "PDL Name", parent_customer_name;
    """
    with get_db_con() as con:
        con.execute(sql)
# def render_table_group(table_list, key_prefix):
#     selected = []
#     if not table_list:
#         st.caption("No tables found in this category.")
#         return selected

#     for table in table_list:
#         cols = st.columns([0.6, 0.2, 0.2])

#         # 1. Selection Checkbox
#         if cols[0].checkbox(table, key=f"sel_{key_prefix}_{table}"):
#             selected.append(table)
#         # 2. Preview
#         if cols[1].button("👁️", key=f"pre_{key_prefix}_{table}", help="Preview Data"):
#             st.session_state.preview_table = table
#             st.rerun()
#         # 3. Delete
#         if cols[2].button("🗑️", key=f"del_{key_prefix}_{table}", help="Delete table"):
#             st.session_state.confirm_delete = table
#             st.rerun()

#     return selected

def render_table_group(table_list, key_prefix):
    selected = []
    if not table_list:
        st.caption("No tables found in this category")
        return selected
    
    for table in table_list:
        cols = st.columns([0.7, 0.1, 0.1, 0.1], vertical_alignment="center")
        
        if cols[0].checkbox(table, key=f"sel_{key_prefix}_{table}"):
            selected.append(table)
        if cols[1].button(":material/visibility:", type="tertiary", key=f"pre_{key_prefix}_{table}", help="Preview Data"):
            st.session_state.preview_table = table
            st.rerun()
        if cols[2].button(":material/upload_file:", type="tertiary", key=f"up_{key_prefix}_{table}", help="Upload & Validate Excel"):
            upload_and_validate_dialog(table)
        if cols[3].button(":material/delete:", type="tertiary", key=f"del_{key_prefix}_{table}", help="Delete table"):
            st.session_state.confirm_delete = table
            st.rerun()
    return selected

@st.dialog("Schema Validator", width="large")
def upload_and_validate_dialog(table_name):
    st.write(f"Updating data for: **{table_name}**")

    with get_db_con_ro() as con:
        schema_df = con.execute(f"DESCRIBE {table_name}").df()
        required_cols = set(schema_df['column_name'].tolist())

    st.info(f"Required Columns: {', '.join(required_cols)}")
    uploaded_file = st.file_uploader("Upload updated Excel file", type=["xlsx", "xlsb"])

    if uploaded_file:
        try:
            df_new = pd.read_excel(uploaded_file, engine='calamine', dtype=str)
            df_new.columns = df_new.columns.str.strip()
            df_new['load_date'] = pd.Timestamp.now(tz='Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S %Z')
            uploaded_cols = set(df_new.columns)
            st.info(f"Uploaded file columns: {uploaded_cols}")
            missing = required_cols - uploaded_cols
            extra = uploaded_cols - required_cols

            if missing:
                st.error(f"❌ **Validation Failed!** Missing columns: {', '.join(missing)}")
                st.warning("Please fix the headers in your Excel file and re-upload.")
            else:
                st.success("✅ **Validation Passed!** All required columns are present.")
                if extra:
                    st.caption(f"Note: Extra columns found and will be included: {', '.join(extra)}")
                if st.button("Confirm & Overwrite Table", type="primary"):
                    with get_db_con() as con:
                        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_new")
                    st.toast(f"Table {table_name} updated successfully!")
                    st.rerun()

        except Exception as e:
            st.error(f"Error reading file: {e}")
    return


@st.dialog("Confirm Deletion")
def confirm_delete_dialog(table_name):
    st.warning(f"Are you sure you want to permanently delete **{table_name}**?")
    c1, c2 = st.columns(2)

    if c1.button("Yes, Delete", type="secondary", width="stretch"):
        try:
            with get_db_con() as con:
                is_view = con.execute(f"SELECT 1 FROM duckdb_views WHERE view_name = '{table_name}'").fetchone()
                if is_view:
                    con.execute(f'DROP VIEW IF EXISTS "{table_name}"')
                else:
                    con.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            del st.session_state.confirm_delete
            st.toast(f"Removed {table_name}")
            st.rerun()

        except Exception as e:
            st.error(f"Error during deletion: {e}")

    if c2.button("Cancel", width="stretch"):
        del st.session_state.confirm_delete
        st.rerun()

@st.dialog("Table Preview", width="large")
def preview_table_dialog(table_name):
    st.write(f"Showing rows from **{table_name}**")

    try:
        with get_db_con_ro() as con:
            df_preview = con.execute(f'SELECT * FROM "{table_name}"').df()
        
        col1, col2 = st.columns([3, 1], vertical_alignment="bottom")
        with col1:
            search_query = st.text_input(
                "🔍 Search table (e.g., PDL, Customer, or Project)",
                  placeholder="Type to filter...",
                  label_visibility="visible"
                  )
        if search_query:
            mask = df_preview.apply(lambda row: row.astype(str).str.contains(search_query, case=False).any(), axis=1)
            df_display = df_preview[mask]
        else:
            df_display = df_preview
        
        with col2:
            if not df_display.empty:
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    df_display.to_excel(writer, index=False, sheet_name='Ops-Data')
            
            st.download_button(
                label="📥Download",
                data=buffer.getvalue(),
                type="primary",
                file_name=f"{table_name}_download.xlsx",
                mime="application/vnd.ms-excel",
                width="stretch"
            )
        st.write(f"Showing {len(df_display)} of {len(df_preview)} records")

        if not df_preview.empty:
            st.dataframe(df_display, width="stretch", hide_index=True)
        else:
            st.info("This table is currently empty.")
    except Exception as e:
        st.info(f"Could not load preview: {e}")
    
    if st.button("Close Preview", width="stretch"):
        #del st.session_state.preview_table
        st.rerun()

# The new Sidebar design
with st.sidebar:
    #st.image("util/media/logo.jpg", use_container_width=True)
    #st.markdown("#### OPS-ASSIST")
    st.title("Ops Assist")
    #st.divider()
    

    # --- ZONE 1: INGESTION ---
    with st.expander("📤 Upload New Data", expanded=False):
        data_ingestion_ui()
    
    #st.divider()
   

    # --- SONE 2: TABLE MANAGEMENT ---
    st.subheader("🗄️ Active Tables")
    all_tables = get_all_tables()

    # Categorize tables based on namming conventions
    transactions = [t for t in all_tables if not t.startswith(('map_', '_ref', 'v_','dashboard','archive', 'arv','pdl_summary'))]
    lookups = [t  for t in all_tables if t.startswith('map_')]
    views = [t for t in all_tables if t.startswith(('pdl', 'dashboard'))]

    # 2a. Transactional Data
    with st.expander("📊 Transactional Tables", expanded=False):
        selected_transactions = render_table_group(transactions, "trans")

    #2b. Lookup / Master Data
    with st.expander("🔍 Lookups & Mappings", expanded=False):
        selected_lookups = render_table_group(lookups, "look")

    with st.expander("⚙️ Adjust Weights", expanded=False):
        st.session_state.fulfilment_weight = st.slider(
                "Fulfillment %", 0.0, 1.0, 0.2, 0.1,
                help="Weightage for fulfilled demands in Effective FTE"
        )
        st.session_state.demand_weight = st.slider(
                "Open Demand %", 0.0, 1.0, 0.5, 0.1,
                help="Weightage for open demands in effective FTE"
        )

    st.subheader("🗄️ Aggreagated Data")

    # 2c. Final Reports (Views)
    with st.expander("🚀 Analytics Views", expanded=False):
        selected_views = render_table_group(views, "view")

    # Final combined list for AI context
    selected_tables = selected_transactions + selected_lookups + selected_views

    #st.divider()

    #st.subheader("⚙️ Calculation Settings")
   
    # --- ZONE 3: ACTIONS ---
    # col1 , col2 = st.columns(2)
    # with col1:
    #     if st.button("🪄 Dashboard", type="primary", width="stretch"):
    #         create_master_report_view(
    #             f_weight=st.session_state.fulfilment_weight,
    #             d_weight=st.session_state.demand_weight
    #         )
    #         st.success("Dashboard Updated!")
    # with col2:
    #     if st.button("📈 PDL View", type="primary", width="stretch"):
    #         create_pdl_summary_view()
    #         st.success("PDL Summary Updated!")
    # st.divider()
    #st.subheader("🕵️ AI Observability")
    
    st.subheader("⚖️ AI Governance") 
    if "phoenix_session" in st.session_state:
        #url = st.session_state.phoenix_session.url
        url = "http://20.84.61.96:6006"
        #st.success("Observability Engine: Online")
        st.link_button("🕵️ Agent Observability", url, type="primary")
    with st.expander("🛠️ SQL Console"):
        query = st.text_area("Paste your Query here:", height=150)
        if st.button("Run Query"):
            with get_db_con() as con:
                res = con.execute(query).df()
                st.dataframe(res)

if "confirm_delete" in st.session_state:
    confirm_delete_dialog(st.session_state.confirm_delete)

if "preview_table" in st.session_state and st.session_state.preview_table:
    table_to_show = st.session_state.preview_table
    st.session_state.preview_table = None
    preview_table_dialog(table_to_show)
    

# Chat interface
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("chart") is not None:
            st.altair_chart(msg["chart"], use_container_width=True)

# Handle Input
if prompt := st.chat_input("Ask a question about the selected data ..."):
    if not selected_tables:
        st.error("No data selected in the sidebar!")
    else:
        # Show user message
        st.session_state.messages.append({"role":"user", "content": prompt})
        with st.chat_message("user", avatar="🧑‍💻"):
            st.markdown(prompt)
        
        with st.chat_message("assistant"):
            status_container = st.status("Analyzing across files...", expanded=False)

            # Pass the list of selected tables to the agent
            inputs = {
                "question": prompt,
                "active_tables": selected_tables
            }
            
            final_response = ""
            current_chart = None

            try:
                # Stream the graph updates
                
                for step in agent_app.stream(inputs):
                    if "generate_query" in step:
                        #status_container.write(f" User Question : {prompt}")
                        status_container.write(f" Generated SQL for : `{step['generate_query']['sql_query']}`")
                    # if "execute_query" in step:
                    #     status_container.write("Executed Query in Local DuckDB Database")
                    # if "generate_plot" in step:
                    #     current_chart = step['generate_plot'].get('chart_spec')
                    #     if current_chart:
                    #         status_container.write("✅ Visualization generated")
                    if "summerize" in step:
                        final_response = step['summerize']['messages'][0]
                
                status_container.update(label="Analysis Complete !", state="complete", expanded=False)
                st.markdown(final_response)
                if current_chart:
                    st.altair_chart(current_chart, width="stretch")
                
                st.session_state.messages.append({"role":"assistant", "content": final_response, "chart": current_chart})
            
            except Exception as e:
                st.error(f"An error occurred: {e}")