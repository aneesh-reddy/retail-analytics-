# insert_user_pyodbc.py
import pyodbc
import os
from dotenv import load_dotenv

# Load .env
load_dotenv()

AZURE_SQL_CONNECTION_STRING = os.getenv("AZURE_SQL_CONNECTION_STRING")

# Connect using pyodbc
conn = pyodbc.connect(AZURE_SQL_CONNECTION_STRING)
cursor = conn.cursor()

# Insert a test user
try:
    cursor.execute(
        "INSERT INTO users (username, password, email) VALUES (?, ?, ?)",
        ("testuser_pyodbc", "testpass123", "testuser_pyodbc@example.com")
    )
    conn.commit()
    print("✅ Inserted user successfully via pyodbc.")
except Exception as e:
    print(f"❌ Error inserting user: {e}")
finally:
    conn.close()
