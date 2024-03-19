import streamlit as st
from elasticsearch import Elasticsearch
import elasticsearch
import time
from datetime import datetime
from elasticsearch_dsl import Search
import os

# Environment variable for API key
es_url = os.getenv('ES_URL')
api_key = os.getenv('ES_API_KEY')

# Initialize Elasticsearch instance
es = Elasticsearch(
    [es_url],
    api_key=api_key
)

index_name = "instructions-demo"

def get_next_document():
    search = Search(using=es, index=index_name).query("match", status="new").sort("_doc")[:1]
    response = search.execute()

    if response.hits.total.value > 0:
        document_id = response[0].meta.id
        update_document_status(document_id, status="in progress")
        return response[0]
    else:
        return None

def update_document_status(doc_id, status, retry_count=10):
    update_body = {"doc": {"status": status, "last_modified": datetime.utcnow().isoformat()}}
    if 'nickname' in st.session_state:
        update_body["doc"]["updated_by"] = st.session_state.nickname
    for attempt in range(retry_count):
        try:
            es.update(index=index_name, id=doc_id, body=update_body)
            break
        except elasticsearch.ConflictError:
            if attempt < retry_count - 1:
                time.sleep(0.5)
                continue
            else:
                raise

def init_or_get_document():
    if 'doc' not in st.session_state or st.session_state.doc is None:
        st.session_state.doc = get_next_document()
        if st.session_state.doc:
            doc_id = st.session_state.doc.meta.id
            update_document_status(doc_id, status="in progress")

def main():
    st.title("Alpaki wychodzą z szafy")

    if 'nickname' not in st.session_state:
        nickname = st.text_input("Enter your nickname:")
        if nickname:
            st.session_state.nickname = nickname
            init_or_get_document()
            st.rerun()
        return

    if 'document_updated' not in st.session_state:
        st.session_state.document_updated = False

    

    if st.session_state.doc and not st.session_state.document_updated:
        doc = st.session_state.doc
        instruction = st.text_area("Instruction", value=doc.instruction.instruction, height=200)
        input_field = st.text_area("Input", value=doc.instruction.input, height=100)
        output_field = st.text_area("Output", value=doc.instruction.output, height=200)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("OK"):
                es.update(index=index_name, id=doc.meta.id, body={
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

if __name__ == "__main__":
    main()
