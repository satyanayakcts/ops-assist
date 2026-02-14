import os
import duckdb
import pandas as pd
import streamlit as st
from typing import TypedDict, List
from langgraph.graph import StateGraph, END
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.output_parsers import StrOutputParser

api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    api_key = st.secrets["passwords"]["GOOGLE_API_KEY"]

db_path = os.getenv("DATABASE_PATH")
if not db_path:
    db_path = st.secrets["passwords"]["DB_PATH"]

# Setup Gemini for now.
# TODO : give option to set the api key from UI 
llm_config = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash", 
    #model="gemini-3-pro-preview", 
    api_key=api_key,
    temperature=0
)

llm = llm_config | StrOutputParser()

# Define the State. This will be the overall state of the agent at runtime
class AgentState(TypedDict):
    question: str
    active_tables: List[str]
    sql_query: str
    query_result: str
    messages: List[str]

# def get_schema():
#     """Helper to get schema from DuckDB for the LLM"""
#     con = duckdb.connect(database=':memory:')

#     con.execute(
#         "CREATE OR REPLACE TABLE data2 AS " \
#         "SELECT * FROM read_xlsx('data2.xlsx')"
#     )
#     schema = con.execute("DESCRIBE data2").fetchall()
#     return str(schema)

# def get_multi_table_schema(tables: List[str]):
#     con = duckdb.connect(database=':memory:')
#     schema_info = []
#     for table in tables:
#         con.execute(f"CREATE TABLE {table} AS SELECT * FROM 'data/{table}.parquet'")
#         columns = con.execute(f"DESCRIBE {table}").fetchall()
#         schema_info.append(f"Table: {table}\nColumns: {columns}")
#     return "\n\n".join(schema_info)

def get_schema(tables: List[str]):
    """Fetches the schema from the database tables"""
    if not tables:
        return "No tables selected."
    con = duckdb.connect(db_path, read_only=True)
    schema_context = []

    for table in tables:
        description = con.execute(f"DESCRIBE {table}").fetchall()
        cols = [f"{col[0]} ({col[1]})" for col in description]
        schema_context.append(f"Table: {table}\nColumns: {', '.join(cols)}")
    
    con.close()
    return "\n\n".join(schema_context)

def generate_query_node(state: AgentState):
    """Node 1: Translate Question to SQL"""
    schema = get_schema(state['active_tables'])
    
    # prompt = f"""
    # You are a high-level Data Analyst for a business leader.
    # Your goal is to write a DuckDB SQL query that is resilient to messy Excel data.

    # DATABASE CONTEXT:
    # {schema}

    # CLEANING RULES (CRITICAL):
    # 1. NUMERIC DATA: Many columns are stored as VARCHAR. They may contain commas (1,200), dashes (-), or currency symbols.
    # 2. RESILIENT SUMS: To sum a column, always use:
    #    SUM(TRY_CAST(REGEXP_REPLACE(column_name, '[^0-9.]', '', 'g') AS DOUBLE))
    # 3. JOINS: If joining tables, use the exact table names provided: {state['active_tables']}
    # 4. CASE INSENSITIVITY: Use ILIKE for string comparisons to handle user typos.

    # OUTPUT:
    # Return ONLY the SQL query. No explanation.

    # USER QUESTION: {state['question']}
    # """

    prompt = f"""
    You are a DuckDB SQL Expert. Write a query to answer the user's question based on the schema below.

    DATABASE SCHEMA:
    {schema}

    IMPORTANT RULES:
    1. All columns are stored as strings. Use TRY_CAST(column_name AS DOUBLE) for any mathematical operations (SUM, AVG, etc.).
    2. Use ILIKE for string comparisons to ensure case-insensitivity.
    3. Use the exact table names provided in the schema.
    4. Output the SQL query as plain text only. Do not use markdown blocks or backticks.

    USER QUESTION: {state['question']}
    """

    response = llm.invoke(prompt)
    #sql = response.replace("```sql", "").replace("```", "").strip()

    return {"sql_query": response}

def execute_query_node(state: AgentState):
    """Node 2: Run SQL in DuckDB"""

    with duckdb.connect(db_path) as con:
        try:
            df_result = con.execute(state['sql_query']).df()

            if df_result.empty:
                result_str = "No data found for this specific query."
            else:
                result_str = df_result.to_string(index=False)

        except Exception as e:
            result_str = f"Execution Error: {str(e)}"
        finally:
            con.close()
        return {"query_result": result_str}
            


    # sql = state['sql_query']
    # con = duckdb.connect(database=':memory:')
    # # Register all active tables in this connections
    # for table in state["active_tables"]:
    #     con.execute(f"CREATE TABLE {table} AS SELECT * FROM 'data/{table}.parquet'")

    # try:
    #     df_result = con.execute(sql).fetchdf()

    #     if df_result.empty:
    #         result_str = "No results found."
    #     else:
    #         result_str = df_result.to_string(index=False)
    
    # except Exception as e:
    #     result_str = f"Error executing SQL: {e}"
    
    # return {"query_result": result_str}

def summerize_insight_node(state: AgentState):
    """Node 3: Human-readable answer"""
    question = state['question']
    result = state['query_result']

    prompt = f"""
    The User asked: {state['question']}
    The SQL query returned this data: {result}
    
    Your Task:
    - Provide a direct answer.
    - If answer can be given in a table format, please do so.
    - If answer has decimal numbers, round it off to a whole number.
    - If the result is '0' or 'No data', explain that the records might be empty or improperly formatted in the source file.
    - Do not simple say "I cannot provide the information" if there is a number available.
    """

    response = llm.invoke(prompt)
    return {"messages": [response]}
    # output = llm.invoke(prompt)
    # response = output.content if hasattr(output, 'content') else output
    # return {"messages": response}

workflow = StateGraph(AgentState)

workflow.add_node("generate_query", generate_query_node)
workflow.add_node("execute_query", execute_query_node)
workflow.add_node("summerize", summerize_insight_node)

workflow.set_entry_point("generate_query")
workflow.add_edge("generate_query", "execute_query")
workflow.add_edge("execute_query", "summerize")
workflow.add_edge("summerize", END)

app = workflow.compile()


# if __name__== "__main__":
#     schema = get_schema()
#     print(f"The schema :: {schema}")
