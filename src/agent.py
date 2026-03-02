import os
import duckdb
import pandas as pd
import streamlit as st
import altair as alt
from typing import TypedDict, List, Optional
from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, END
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.messages import HumanMessage

# from openinference.instrumentation.langchain import LangChainInstrumentor
# LangChainInstrumentor().instrument(skip_if_installed=True)

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

class ChartSpec(BaseModel):
    chart_type: str = Field(description="bar, line or pie")
    x_axis: str = Field(description="Column name for X axis")
    y_axis: str = Field(description="Column name for Y axis")
    title: str = Field(description="Clear, business oriented title for the chart")
    #data_json: str = Field(description="JSON string for data list to be plotted")


# Define the State. This will be the overall state of the agent at runtime
class AgentState(TypedDict):
    question: str
    active_tables: List[str]
    sql_query: str
    query_result: List[dict]
    result_str: str
    messages: List[str]
    chart_spec: Optional[alt.Chart]


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

    prompt = f"""
    You are a DuckDB SQL Expert. Write a query to answer the user's question based on the schema below.

    DATABASE SCHEMA:
    {schema}

    IMPORTANT RULES:
    1. All columns are stored as strings. Use TRY_CAST(column_name AS DOUBLE) for any mathematical operations (SUM, AVG, etc.).
    2. Use ILIKE for string comparisons to ensure case-insensitivity. Also put % before and after when you are doing string comparison with ILIKE.
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
                return {
                    "query_result": [], 
                    "result_summary": "No data found."
                }  
            # else:
            #     result_str = df_result.to_string(index=False)
            raw_data = df_result.to_dict(orient="records")
            formatted_text = df_result.to_string(index=False)
            return {
                "query_result": raw_data,
                "result_str": formatted_text
            }
        except Exception as e:
            return {
                "query_result": [],
                "result_str": f"Error: {str(e)}"
            }
            
def plotting_node(state: AgentState):
    data = state.get("query_result", [])

    print(f"DEBUG: Plotting Node data length: {len(data)}")
    # If the query result has less than 3 rows, dont generate chart
    if not data or len(data) < 3:
        return {"chart_spec": None}

    df = pd.DataFrame(data)
    headers = list(df.columns)

    # Force the LLM to understand the data and return chart_type, x-axis, y-axis and title
    visualizer = llm_config.with_structured_output(ChartSpec)

    system_prompt = f"""
    You are a BI expert. Based on these columns: {headers}, 
    select the best X and Y axis to answer the user's question.
    - If comparing categories: 'bar'
    - If showing trends: 'line'
    Return null if a chart is not helpful
    """

    user_prompt = f"Question: {state['question']} \nData Preview: {data[:3]}"

    try:
        # LLM acts as architect and picks the columns for plotting
        spec = visualizer.invoke([ ("system", system_prompt), ("human", user_prompt)])
        if not spec: return {"chart_spec": None}

        #Python acts a buider for generating chart
        df[spec.y_axis] = pd.to_numeric(df[spec.y_axis], errors='coerce')

        chart = alt.Chart(df).mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3).encode(
            #x=alt.X(f"{spec.x_axis}:N", sort='-y', title=spec.x_axis.replace("_", " ")),
            x=alt.X(f"{spec.x_axis}:N", title=spec.x_axis.replace("_", " ")),
            y=alt.Y(f"{spec.y_axis}:Q", title=spec.y_axis.replace("_", " ")),
            tooltip=headers,
            color=alt.value("#4C78A8")
        ).properties(
            title=spec.title,
            width='container',
            height=350
        ).configure_title(anchor='start', fontSize=18)
        print("DEBUG: Chart created successfully!")
        return {"chart_spec": chart}
    except Exception as e:
        print(f"Plotting Error: {e}")
        return {"chart_spec": None}



def summerize_insight_node(state: AgentState):
    """Node 3: Human-readable answer"""
    question = state['question']
    result = state['result_str']
    chart_present = state.get('chart_spec') is not None

    prompt = f"""
    The User asked: {question}
    The SQL query returned this data: {result}
    Chart Included: {'Yes' if chart_present else 'No'}
    
    Your Task:
    - Provide a direct answer.
    - If answer can be given in a table format, please do so.
    - If answer has decimal numbers, round it off to a whole number.
    - Do not simple say "I cannot provide the information" if there is a number available.
    - When presenting graph, exclude null value data points.
    """
    # - If the result is '0' or 'No data', explain that the records might be empty or improperly formatted in the source file.

    response = llm.invoke(prompt)
    return {"messages": [response]}
    # output = llm.invoke(prompt)
    # response = output.content if hasattr(output, 'content') else output
    # return {"messages": response}

# def summerize_insight_node(state: AgentState):
#     summary_prompt = f"""
#     Data: {state['result_str']}
#     User Question: {state['question']}
#     Chart Included: {'Yes' if state['chart_spec'] else 'No'}
    
#     Summarize the findings for a business leader. 
#     If a chart is included, refer to it (e.g., 'As shown in the chart above...').
#     Keep it concise and focus on KPIs (Headcount, FTE, Cost).
#     """
    
#     response = llm.invoke(summary_prompt)
#     #return {"final_summary": response.content}
#     return {"messages": [response]}

workflow = StateGraph(AgentState)

workflow.add_node("generate_query", generate_query_node)
workflow.add_node("execute_query", execute_query_node)
workflow.add_node("generate_plot", plotting_node)
workflow.add_node("summerize", summerize_insight_node)

workflow.set_entry_point("generate_query")
workflow.add_edge("generate_query", "execute_query")

workflow.add_edge("execute_query", "generate_plot")
workflow.add_edge("generate_plot", "summerize")

# workflow.add_edge("execute_query", "summerize")

workflow.add_edge("summerize", END)

app = workflow.compile()


# if __name__== "__main__":
#     # schema = get_schema()
#     # print(f"The schema :: {schema}")
#     state = {
#         "question":"show me the details of all the open demands sbu wise", 
#         "active_tables":['dashboard12'],
#         "sql_query":"",
#         "query_result": "",

#         }
#     chartspec = plotting_node()
