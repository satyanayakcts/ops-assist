# import pandas as pd
# from phoenix.client import Client
# from phoenix.evals import GeminiModel, llm_classify, RAG_RELEVANCY_PROMPT_TEMPLATE

# def run_audit():
#     client = Client()
#     spans_df = client.get_spans_dataframe(project_name="ops-assist")

#     if spans_df.empty:
#         return "No traces found"

#     eval_model = GeminiModel(model="gemini-2.5-flash")
#     eval_result = llm_classify(
#         data=spans_df,
#         template=RAG_RELEVANCY_PROMPT_TEMPLATE,
#         model=eval_model,
#         rails=["relevant", "irrevalent"]
#     )

#     from phoenix.evals.utils import to_annotation_dataframe
#     client.log_span_annotations_dataframe(to_annotation_dataframe(eval_result))
#     return eval_result