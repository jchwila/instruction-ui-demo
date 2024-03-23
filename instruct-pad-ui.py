import os
import pandas as pd
import plotly.express as px
from datetime import datetime
from elasticsearch import Elasticsearch, ConflictError
from elasticsearch_dsl import Search
import streamlit as st
import time

ES_URL = os.getenv('ES_URL')
ES_API_KEY = os.getenv('ES_API_KEY')

INDEX_NAME = "instructions-demo"

def create_es_client():
    return Elasticsearch([ES_URL], api_key=ES_API_KEY)

es = create_es_client()


PROGRESS_QUERY = {
    "size": 0,  
    "aggs": {
        "results": {
            "terms": {
                "field": "status.keyword"
            }
        }
    }
}

LEADERS_QUERY = {
    "size": 0,  
    "aggs": {
        "results": {
            "terms": {
                "field": "updated_by.keyword"
            }
        }
    }
}

def fetch_aggregation_results(query):
    response = es.search(index=INDEX_NAME, body=query)
    buckets = response["aggregations"]["results"]["buckets"]
    return pd.DataFrame(buckets).rename(columns={'key': 'Name', 'doc_count': 'Count'}).sort_values(by='Count', ascending=False)

def calculate_progress():
    df = fetch_aggregation_results(PROGRESS_QUERY)
    total_docs = df['Count'].sum()
    total_except_new = df[df['Name'] != 'new']['Count'].sum()
    return total_except_new / total_docs if total_docs else 0

def leaderboard_df():
    df = fetch_aggregation_results(LEADERS_QUERY)
    return df.sort_values(by='Count', ascending=False)

def get_next_document():
    search = Search(using=es, index=INDEX_NAME).query("match", status="new").sort("_doc")[:1]
    response = search.execute()
    if response.hits.total.value > 0:
        document = response[0]
        update_document_status(document.meta.id, "in progress")
        return document
    return None

def update_document_status(doc_id, status, retry_count=5):
    update_body = {"doc": {"status": status, "last_modified": datetime.utcnow().isoformat()}}
    if 'nickname' in st.session_state:
        update_body["doc"]["updated_by"] = st.session_state['nickname']
    for attempt in range(retry_count):
        try:
            es.update(index=INDEX_NAME, id=doc_id, body=update_body)
            break
        except ConflictError:
            if attempt < retry_count - 1:
                time.sleep(0.5)
            else:
                raise

def init_or_update_document():
    if 'doc' not in st.session_state or st.session_state.doc is None:
        st.session_state.doc = get_next_document()

def main():
    st.title("[TEST] Speakleash Instruction Pad")
    current_progress = calculate_progress() + 0.63
    st.write(f"Total Instructions Progress: {current_progress:.2%}")
    st.progress(current_progress)

    tab1, tab2 = st.tabs(["Instructions", "Leaderboard"])
    with tab1:
        manage_instructions_tab()
    with tab2:
        display_leaderboard_tab()

def manage_instructions_tab():
    if 'nickname' not in st.session_state:
        if st.button("Get Next Instruction"):
            st.session_state.nickname = st.experimental_user.email
            init_or_update_document()
            st.rerun()
        return

    if 'document_updated' not in st.session_state:
        st.session_state.document_updated = False

    if st.session_state.doc and not st.session_state.document_updated:
        doc = st.session_state.doc
        st.title(st.session_state.nickname)
        instruction = st.text_area("Instruction", value=doc.instruction.instruction, height=200)
        input_field = st.text_area("Input", value=doc.instruction.input, height=100)
        output_field = st.text_area("Output", value=doc.instruction.output, height=200)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("OK"):
                es.update(index=INDEX_NAME, id=doc.meta.id, body={
                    "doc": {
                        "instruction": {"instruction": instruction, "input": input_field, "output": output_field},
                        "status": "ok",
                        "last_modified": datetime.utcnow().isoformat(),
                        "updated_by": st.session_state.nickname 
                    }
                })
                st.session_state.doc = None
                st.session_state.document_updated = True
                st.rerun()

        with col2:
            if st.button("NOT OK"):
                update_document_status(doc.meta.id, status="not ok")
                st.session_state.doc = None
                st.session_state.document_updated = True
                st.rerun()

    if st.session_state.document_updated:
        st.success("Instruction updated successfully!")
        if st.button("Get Next Instruction"):
            st.session_state.document_updated = False
            st.session_state.doc = get_next_document()
            if st.session_state.doc:
                doc_id = st.session_state.doc.meta.id
                update_document_status(doc_id, status="in progress")
            st.rerun()
    elif not st.session_state.doc:
        st.write("No new Instructions found.")

def display_leaderboard_tab():
    st.write("Leaderboard")
    leaderboard_data = leaderboard_df()
    fig = px.bar(leaderboard_data, x='Count', y='Name', orientation='h')
    fig.update_yaxes(categoryorder='total ascending')
    fig.update_layout(xaxis_title='Instructions Count', yaxis_title='User')
    st.plotly_chart(fig)

if __name__ == "__main__":
    main()
