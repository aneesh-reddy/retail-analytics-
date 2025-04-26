# app.py

import streamlit as st
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

AZURE_SQL_CONNECTION_STRING = os.getenv("AZURE_SQL_CONNECTION_STRING")

# Setup SQLAlchemy engine
engine = create_engine(AZURE_SQL_CONNECTION_STRING)

# ────────────────────────────────
# Streamlit Page Config
st.set_page_config(page_title="Retail Analytics Web App", page_icon="🛍️", layout="wide")

# ────────────────────────────────
# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Login", "Data Pull", "Upload Data", "Dashboard"])

# ────────────────────────────────
# Login Page
if page == "Login":
    st.title("🛒 Retail Analytics Login")

    with st.form(key="login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        email = st.text_input("Email Address")
        submit = st.form_submit_button(label="Submit")

    if submit:
        st.success(f"Welcome {username}!")
        st.info("Use the sidebar to navigate to other pages.")

# ────────────────────────────────
# Data Pull Page
elif page == "Data Pull":
    st.title("🔍 Search Data by HSHD_NUM")

    hshd_num = st.text_input("Enter HSHD_NUM to search:")

    if st.button("Search"):
        if hshd_num:
            query = f"""
                SELECT 
                    h.HSHD_NUM, 
                    t.BASKET_NUM, 
                    t.PURCHASE_DATE, 
                    t.PRODUCT_NUM, 
                    p.DEPARTMENT, 
                    p.COMMODITY
                FROM 
                    transactions t
                JOIN 
                    households h ON t.HSHD_NUM = h.HSHD_NUM
                JOIN 
                    products p ON t.PRODUCT_NUM = p.PRODUCT_NUM
                WHERE 
                    h.HSHD_NUM = {hshd_num}
                ORDER BY 
                    h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_DATE, p.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY
            """
            try:
                df = pd.read_sql_query(query, con=engine)
                st.dataframe(df)
            except Exception as e:
                st.error(f"Error querying database: {e}")

# ────────────────────────────────
# Upload Data Page
elif page == "Upload Data":
    st.title("📤 Upload New Datasets")

    uploaded_households = st.file_uploader("Upload Households CSV", type="csv")
    uploaded_transactions = st.file_uploader("Upload Transactions CSV", type="csv")
    uploaded_products = st.file_uploader("Upload Products CSV", type="csv")

    if st.button("Upload and Load to Azure SQL"):
        try:
            if uploaded_households:
                df_hh = pd.read_csv(uploaded_households)
                df_hh.to_sql("households", con=engine, if_exists="replace", index=False)
                st.success("Households table updated!")

            if uploaded_transactions:
                df_tx = pd.read_csv(uploaded_transactions)
                df_tx.to_sql("transactions", con=engine, if_exists="replace", index=False)
                st.success("Transactions table updated!")

            if uploaded_products:
                df_pd = pd.read_csv(uploaded_products)
                df_pd.to_sql("products", con=engine, if_exists="replace", index=False)
                st.success("Products table updated!")

        except Exception as e:
            st.error(f"Error loading data: {e}")

# ────────────────────────────────
# Dashboard Page
elif page == "Dashboard":
    st.title("📊 Retail Analytics Dashboard")

    st.subheader("1. Top Departments")
    try:
        df_dept = pd.read_sql_query("SELECT DEPARTMENT, COUNT(*) AS count FROM products GROUP BY DEPARTMENT ORDER BY count DESC LIMIT 10", con=engine)
        st.bar_chart(df_dept.set_index("DEPARTMENT"))
    except Exception as e:
        st.error(f"Error loading dashboard data: {e}")

    st.subheader("2. Household Sizes")
    try:
        df_households = pd.read_sql_query("SELECT HSHD_NUM FROM households", con=engine)
        st.line_chart(df_households['HSHD_NUM'].value_counts())
    except Exception as e:
        st.error(f"Error loading households data: {e}")

