import streamlit as st
from snowflake.snowpark.session import Session


# Connect to Snowflake
@st.cache_resource
def connect_to_snowflake():
    connection_parameters = {
        "account": st.secrets["snowflake_account"],
        "user": st.secrets["snowflake_user"],
        "password": st.secrets["snowflake_password"],
        "role": st.secrets["snowflake_role"],
        "warehouse": st.secrets["snowflake_warehouse"],
        "database": st.secrets["snowflake_database"],
        "schema": st.secrets["snowflake_schema"],
    }
    return Session.builder.configs(connection_parameters).create()
