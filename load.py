#!/usr/bin/env python3
import os
import urllib
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine

load_dotenv()

# ─── Configuration ──────────────────────────────────────────────────────────────
STORAGE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
SQL_CONN_STR      = os.getenv("AZURE_SQL_CONNECTION_STRING")
BLOB_CONTAINER    = os.getenv("AZURE_BLOB_CONTAINER_NAME", "rawdata")
LOCAL_DATA_DIR    = "data/raw"
# ────────────────────────────────────────────────────────────────────────────────

def download_blobs():
    """Download every blob under the container into data/raw/..."""
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
    container = client.get_container_client(BLOB_CONTAINER)

    for blob in container.list_blobs():
        dest = os.path.join(LOCAL_DATA_DIR, blob.name)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "wb") as f:
            f.write(container.download_blob(blob).readall())
        print(f"Downloaded: {blob.name}")

def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace, replace spaces with _, and truncate names to 128 chars."""
    clean = []
    for c in df.columns:
        c2 = c.strip().replace(" ", "_")
        if len(c2) > 128:
            c2 = c2[:128]
        clean.append(c2)
    df.columns = clean
    return df

def load_into_sql():
    """Read CSVs, sanitize, and push to Azure SQL in 1k‐row chunks."""
    # Build SQLAlchemy engine with fast_executemany
    odbc = urllib.parse.quote_plus(SQL_CONN_STR)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={odbc}",
                           connect_args={"fast_executemany": True})

    # 1) Households
    df_h = pd.read_csv(os.path.join(LOCAL_DATA_DIR, "400_households.csv"))
    df_h = sanitize_columns(df_h)
    df_h.to_sql("households", engine, if_exists="replace", index=False, chunksize=1000)

    # 2) Products
    df_p = pd.read_csv(os.path.join(LOCAL_DATA_DIR, "400_products.csv"))
    df_p = sanitize_columns(df_p)
    df_p.to_sql("products", engine, if_exists="replace", index=False, chunksize=1000)

    # 3) Transactions (top 10 000 only)
    df_t = pd.read_csv(
        os.path.join(LOCAL_DATA_DIR, "400_transactions.csv"),
        parse_dates=["PURCHASE_DATE"],  # adjust to your actual date column
        infer_datetime_format=True
    ).head(10000)
    df_t = sanitize_columns(df_t)
    df_t.to_sql("transactions", engine, if_exists="replace", index=False, chunksize=1000)

    print("✅ All tables written to Azure SQL.")

if __name__ == "__main__":
    download_blobs()
    load_into_sql()
