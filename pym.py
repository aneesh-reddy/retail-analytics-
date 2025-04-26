# insert_user_pymssql.py
import pymssql
import os
from dotenv import load_dotenv

# Load .env
load_dotenv()

# Parse connection manually (because pymssql expects parts)
server = "retailsqlsrv29.database.windows.net"
user = "sqladmin@retailsqlsrv29"
password = "YourStrongP@ss!"
database = "RetailDB"

# Connect using pymssql
conn = pymssql.connect(server=server, user=user, password=password, database=database)
cursor = conn.cursor()

# Insert a test user
try:
    cursor.execute(
        "INSERT INTO users (username, password, email) VALUES (%s, %s, %s)",
        ("testuser_pymssql", "testpass123", "testuser_pymssql@example.com")
    )
    conn.commit()
    print("✅ Inserted user successfully via pymssql.")
except Exception as e:
    print(f"❌ Error inserting user: {e}")
finally:
    conn.close()
