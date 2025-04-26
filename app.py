import streamlit as st
from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
AZURE_SQL_CONNECTION_STRING = os.getenv("AZURE_SQL_CONNECTION_STRING")

# SQLAlchemy engine
engine = create_engine(AZURE_SQL_CONNECTION_STRING)

# Session state
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "username" not in st.session_state:
    st.session_state.username = None

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
                        text("INSERT INTO users (username, password, email) VALUES (:u, :p, :e)"),
                        {"u": username, "p": password, "e": email}
                    )
                st.success("Registered successfully! You can now log in.")
            except Exception as e:
                st.error(f"Registration failed: {e}")
        else:
            st.error("Please fill out all fields.")

def login():
    st.title("Login")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if username and password:
            with engine.begin() as conn:
                result = conn.execute(
                    text("SELECT * FROM users WHERE username = :u AND password = :p"),
                    {"u": username, "p": password}
                ).fetchone()
                if result:
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.success(f"Welcome, {username}!")
                else:
                    st.error("Invalid username or password.")
        else:
            st.error("Please enter both username and password.")

def get_data(hshd_num):
    query = f"""
        SELECT *
        FROM transactions t
        JOIN households h ON t.HSHD_NUM = h.HSHD_NUM
        JOIN products p ON t.PRODUCT_NUM = p.PRODUCT_NUM
        WHERE t.HSHD_NUM = {hshd_num}
        ORDER BY t.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_DATE, p.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY
    """
    return pd.read_sql(query, engine)

def data_lookup():
    st.title("Retail Data Lookup")

    hshd_num = st.text_input("Enter HSHD_NUM to search")

    if st.button("Search"):
        if hshd_num:
            df = get_data(hshd_num)
            if not df.empty:
                st.dataframe(df)
            else:
                st.warning("No records found for this HSHD_NUM.")
        else:
            st.error("Please enter a valid HSHD_NUM.")

def main():
    if not st.session_state.logged_in:
        choice = st.sidebar.selectbox("Choose Action", ["Login", "Register"])

        if choice == "Login":
            login()
        elif choice == "Register":
            register()
    else:
        st.sidebar.success(f"Logged in as {st.session_state.username}")
        data_lookup()

if __name__ == "__main__":
    main()
