import pandas as pd
from esdatahandler import ElasticsearchDataHandler
import plotly.express as px
from datetime import datetime
import streamlit as st
import os

ES_URL = os.getenv('ES_URL')
ES_API_KEY = os.getenv('ES_API_KEY')
INDEX_NAME = "instructions-demo"

es = ElasticsearchDataHandler(ES_URL, ES_API_KEY, INDEX_NAME)


def main():
    st.title("[TEST] Speakleash Instruction Pad")
    if 'available_scripts' not in st.session_state:
        st.session_state.available_scripts=es.get_scripts()
        st.session_state.selected_script = st.session_state.available_scripts[0]

    current_progress = es.calculate_progress(st.session_state.selected_script) + 0.63
    st.write(f"Total Instructions Progress for {st.session_state.selected_script}: {current_progress:.2%}")
    st.progress(current_progress)

    tab1, tab2 = st.tabs(["Instructions", "Leaderboard"])
    with tab1:
        manage_instructions_tab()
    with tab2:
        display_leaderboard_tab()

def manage_instructions_tab():
    if 'nickname' not in st.session_state:
        selected_script = st.selectbox(
            'Choose an instruction set:',
            st.session_state.available_scripts,
            index=st.session_state.available_scripts.index(st.session_state.selected_script)
            )
        if st.session_state.selected_script != selected_script:
            st.session_state.selected_script = selected_script
            st.rerun()

        if st.button("Get Next Instruction"):
            st.session_state.nickname = st.experimental_user.email
            if 'doc' not in st.session_state or st.session_state.doc is None:
                st.session_state.doc = es.get_next_document(st.session_state.selected_script, st.session_state.nickname)
            st.rerun()
        return

    if 'document_updated' not in st.session_state:
        st.session_state.document_updated = False

    if st.session_state.doc and not st.session_state.document_updated:
        doc = st.session_state.doc
        st.title(st.session_state.nickname)
        instruction = st.text_area("Instruction", value=doc.instruction.instruction, height=200)
        input_field = st.text_area("Input", value=doc.instruction.input, height=200)
        output_field = st.text_area("Output", value=doc.instruction.output, height=200)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("OK"):
                es.update_document(doc.meta.id, update_body={
                    "doc": {
                        "instruction": {"instruction": instruction, "input": input_field, "output": output_field},
                        "status": "ok",
                        "last_modified": datetime.now().isoformat(),
                        "updated_by": st.session_state.nickname 
                    }
                })
                st.session_state.doc = None
                st.session_state.document_updated = True
                st.rerun()

        with col2:
            if st.button("NOT OK"):
                es.update_document_status(doc.meta.id, status="not ok", nickname=st.session_state.nickname)
                st.session_state.doc = None
                st.session_state.document_updated = True
                st.rerun()

    if st.session_state.document_updated:
        st.success("Instruction updated successfully!")
        selected_script = st.selectbox(
            'Choose an instruction set:',
            st.session_state.available_scripts,
            index=st.session_state.available_scripts.index(st.session_state.selected_script)
            )
        if st.session_state.selected_script != selected_script:
            st.session_state.selected_script = selected_script
            st.rerun()
        if st.button("Get Next Instruction"):            
            st.session_state.document_updated = False
            st.session_state.doc = es.get_next_document(st.session_state.selected_script, st.session_state.nickname)
            st.rerun()
    elif not st.session_state.doc:
        st.write("No new Instructions found.")

def display_leaderboard_tab():
    st.write("Leaderboard")
    leaderboard_data = es.leaderboard_df(st.session_state.selected_script)
    fig = px.bar(leaderboard_data, x='doc_count', y='key', orientation='h')
    fig.update_yaxes(categoryorder='total ascending')
    fig.update_layout(xaxis_title='Instructions Count', yaxis_title='User')
    st.plotly_chart(fig)

if __name__ == "__main__":
    main()
