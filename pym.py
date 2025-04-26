# app.py
import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()
AZURE_SQL_CONNECTION_STRING = os.getenv("AZURE_SQL_CONNECTION_STRING")

# Setup SQLAlchemy engine (pymssql driver inside SQLAlchemy)
engine = create_engine(AZURE_SQL_CONNECTION_STRING)

# Session State Setup
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "username" not in st.session_state:
    st.session_state.username = None

# Register Page
def register():
    st.title("Register")

    username = st.text_input("Create Username")
    password = st.text_input("Create Password", type="password")
    email = st.text_input("Enter Email")

    if st.button("Register Now"):
        if username and password and email:
            try:
                with engine.begin() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO users (username, password, email)
                            VALUES (:username, :password, :email)
                        """),
                        {"username": username, "password": password, "email": email}
                    )
                st.success("✅ Registered successfully! Please log in.")
                st.session_state.logged_in = True
                st.session_state.username = username
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Registration failed: {e}")
        else:
            st.error("⚠️ Please fill out all fields!")

# Login Page
def login():
    st.title("Login")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if username and password:
            try:
                with engine.begin() as conn:
                    result = conn.execute(
                        text("""
                            SELECT * FROM users WHERE username = :username AND password = :password
                        """),
                        {"username": username, "password": password}
                    ).fetchone()

                if result:
                    st.success(f"✅ Welcome, {username}!")
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.experimental_rerun()
                else:
                    st.error("❌ Invalid username or password.")
            except Exception as e:
                st.error(f"Login failed: {e}")
        else:
            st.error("⚠️ Please enter both username and password.")

# Fetch Data for HSHD_NUM
def get_data(hshd_num):
    query = """
        SELECT *
        FROM transactions t
        JOIN households h ON t.HSHD_NUM = h.HSHD_NUM
        JOIN products p ON t.PRODUCT_NUM = p.PRODUCT_NUM
        WHERE t.HSHD_NUM = :hshd_num
        ORDER BY t.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_DATE, p.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(query), conn, params={"hshd_num": hshd_num})
    return df

# Data Lookup Page
def data_lookup():
    st.title("Retail Data Lookup")

    hshd_num = st.text_input("Enter HSHD_NUM to search")

    if st.button("Search"):
        if hshd_num:
            try:
                df = get_data(hshd_num)
                if not df.empty:
                    st.dataframe(df)
                else:
                    st.warning("⚠️ No records found for this HSHD_NUM.")
            except Exception as e:
                st.error(f"Error fetching data: {e}")
        else:
            st.error("⚠️ Please enter a valid HSHD_NUM.")

# Main Logic
def main():
    st.sidebar.title("Navigation")

    if not st.session_state.logged_in:
        page = st.sidebar.selectbox("Choose Action", ["Login", "Register"])
        if page == "Login":
            login()
        elif page == "Register":
            register()
    else:
        st.sidebar.success(f"Logged in as {st.session_state.username}")
        data_lookup()

if __name__ == "__main__":
    main()
