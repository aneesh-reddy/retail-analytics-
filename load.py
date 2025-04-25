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
    download_blobs()
    hfile, tfile, pfile = discover_files()

    print("▶️ Reading & sanitizing CSVs…")
    df_h = sanitize(pd.read_csv(hfile))
    df_p = sanitize(pd.read_csv(pfile))
    df_t = (
        sanitize(
            pd.read_csv(
                tfile,
                parse_dates=["PURCHASE_"],
                nrows=10_000
            )
        )
        .rename(columns={"PURCHASE_": "PURCHASE_DATE"})
    )

    print("▶️ Loading into Azure SQL…")
    # turn on fast_executemany for pyodbc
    engine = create_engine(
        SQL_CONN,
        connect_args={"fast_executemany": True}
    )

    with engine.begin() as conn:
        df_h.to_sql(
            "households", conn,
            if_exists="replace", index=False,
            chunksize=1_000
        )
        df_p.to_sql(
            "products", conn,
            if_exists="replace", index=False,
            chunksize=1_000
        )
        df_t.to_sql(
            "transactions", conn,
            if_exists="replace", index=False,
            chunksize=1_000
        )

    print("✅ All tables loaded successfully.")

