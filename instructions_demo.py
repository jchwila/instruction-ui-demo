import streamlit as st
from elasticsearch import Elasticsearch
import elasticsearch
import time
from elasticsearch_dsl import Search
import os


api_key = os.getenv('ES_API_KEY')



es = Elasticsearch(
    ['https://elastic.speakleash.org.pl:443'],
    api_key=api_key
)

index_name = "instructions"  

def get_next_document():
    """Query Elasticsearch for the next document with status 'new'."""
    print('Fetching next document...')
    search = Search(using=es, index=index_name).query("match", status="new").sort("_doc")[:1]
    response = search.execute()

    if response.hits.total.value > 0:
        document_id = response[0].meta.id
        print(document_id)
        update_document_status(document_id, status="in progress")
        return response[0]
    else:
        return None

def update_document_status(doc_id, status, retry_count=10):
    """Update the document's status."""
    for attempt in range(retry_count):
        try:
            es.update(index=index_name, id=doc_id, body={"doc": {"status": status}})
            break
        except elasticsearch.ConflictError:
            if attempt < retry_count - 1:
                time.sleep(0.5)
                continue
            else:
                raise

def init_or_get_document():
    """Initialize or retrieve the document from session state."""
    if 'doc' not in st.session_state or st.session_state.doc is None:
        st.session_state.doc = get_next_document()
        if st.session_state.doc:
            doc_id = st.session_state.doc.meta.id
            update_document_status(doc_id, status="in progress")

def main():
    """Streamlit app main function."""
    st.title("Alpaki wychodzą z szafy")

    if 'document_updated' not in st.session_state:
        st.session_state.document_updated = False

    init_or_get_document()

    if st.session_state.doc and not st.session_state.document_updated:
        doc = st.session_state.doc
        instruction = st.text_area("Instruction", value=doc.instruction.instruction, height=200)
        input_field = st.text_area("Input", value=doc.instruction.input, height=100)
        output_field = st.text_area("Output", value=doc.instruction.output, height=100)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("OK"):
                es.update(index=index_name, id=doc.meta.id, body={
                    "doc": {
                        "instruction": {"instruction": instruction, "input": input_field, "output": output_field},
                        "status": "ok"
                    }
                })
                st.session_state.doc = None
                st.session_state.document_updated = True
                st.experimental_rerun()

        with col2:
            if st.button("NOT OK"):
                update_document_status(doc.meta.id, status="not ok")
                st.session_state.doc = None
                st.session_state.document_updated = True
                st.experimental_rerun()

    if st.session_state.document_updated:
        st.success("Document updated successfully!")
        if st.button("Get Next Document"):
            st.session_state.document_updated = False
            st.session_state.doc = get_next_document()
            if st.session_state.doc:
                doc_id = st.session_state.doc.meta.id
                update_document_status(doc_id, status="in progress")
            st.experimental_rerun()
    elif not st.session_state.doc:
        st.write("No new documents found.")

if __name__ == "__main__":
    main()