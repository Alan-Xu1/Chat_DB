import streamlit as st
import pandas as pd
import numpy as np
from build_db import df_to_sql
from input_process import input_process

if "message" not in st.session_state:
    st.session_state.message = []

if "uploads" not in st.session_state:
    st.session_state.uploads = []

if "setup" not in st.session_state:
    st.session_state.setup = "initial"

if "db_dict" not in st.session_state:
    st.session_state.db_dict = []

if "db_name" not in st.session_state:
    st.session_state.db_name = ""

if "table_name" not in st.session_state:
    st.session_state.table_name = []

if "columns" not in st.session_state:
    st.session_state.columns = {}

def add_message(role, content):
    st.session_state.message.append({"role": role, "content": content})

if st.session_state.setup == "initial":
    file = st.file_uploader("Upload files", accept_multiple_files=True)
    if file:
        try:
            temp = []
            for i in file:
                df = pd.read_csv(i)
                temp.append(df)
                st.session_state.columns[i.name.replace('.csv', '')] = df.columns.astype(str).tolist()
                st.session_state.table_name.append(i.name.replace('.csv', ''))
            st.session_state.uploads = temp
            st.session_state.setup = "awaiting_db_name"
            add_message("assistant", "Please provide a name for your database")
        except:
            add_message("assistant", "Failed to read files. Check your file format!")

if prompt := st.chat_input("Say Something", key="normal"):
    add_message("user", prompt)
    if not st.session_state.uploads:
        add_message("assistant", "Please upload a CSV file to build a database system to explore.")
    if st.session_state.setup == "awaiting_db_name":
        st.session_state.db_to_setup = prompt
        st.session_state.db_name = prompt
        df_to_sql(st.session_state.uploads, st.session_state.table_name, prompt)
        st.session_state.setup = "set"
    if st.session_state.setup == "set":
        add_message("assistant", "You are now able to ask questions.")
        add_message("assistant", "Type explore to get look around the database")
        add_message("assistant", f"You have following tables {st.session_state.table_name}")
        for i,j in st.session_state.columns.items():
            add_message("assistant", f"For table {i} you have following columns {j}")
        st.session_state.setup = "conversation"
    elif st.session_state.setup == "conversation":
        sql_obj = input_process(st.session_state.db_name)
        response = sql_obj.nl_sql(prompt, st.session_state.table_name, st.session_state.columns)
        add_message("assistant", f'The translated query is {response[0]}')
        add_message("assistant", response[1])

col1, col2, col3 = st.columns(3)
with col2:
    if st.button("Start New Session"):
        st.session_state.message = []
        st.session_state.uploads = []
        st.session_state.setup = "initial"
        st.session_state.db_dict = {}
        st.session_state.db_key = {}
        st.session_state.db_to_setup = ""
        st.session_state.table_name = []
        st.session_state.columns = {}
        st.session_state.db_name = ""
        st.rerun()

for message in st.session_state.message:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])


