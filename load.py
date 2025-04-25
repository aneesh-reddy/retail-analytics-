# load.py

import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from azure.storage.blob import ContainerClient

# ─── 1) Load secrets from .env ────────────────────────────────────────────────
load_dotenv()  
AZ_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
SQL_CONN_STR = os.getenv("AZURE_SQL_CONNECTION_STRING")  
# e.g. in .env:
# AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=…;AccountKey=…;EndpointSuffix=core.windows.net"
# AZURE_SQL_CONNECTION_STRING="mssql+pymssql://sqladmin:YourStrongP%40ss%21@retailsqlsrv29.database.windows.net:1433/RetailDB"

# ─── 2) Blob download setup ───────────────────────────────────────────────────
CONTAINER_NAME = "rawdata"
LOCAL_RAW      = "data/raw"

def download_blobs():
    client = ContainerClient.from_connection_string(AZ_CONN_STR, container_name=CONTAINER_NAME)
    for blob in client.list_blobs():
        local_path = os.path.join(LOCAL_RAW, blob.name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(client.get_blob_client(blob).download_blob().readall())
    print("✅ Blobs downloaded to", LOCAL_RAW)

# ─── 3) SQL load via pymssql ──────────────────────────────────────────────────
def load_into_sql():
    engine = create_engine(SQL_CONN_STR)
    # read in your CSVs
    df_h = pd.read_csv(os.path.join(LOCAL_RAW, "400_households.csv"))
    df_p = pd.read_csv(os.path.join(LOCAL_RAW, "400_products.csv"))
    df_t = pd.read_csv(os.path.join(LOCAL_RAW, "400_transactions.csv"),
                       parse_dates=["PURCHASE_"])

    # write to SQL (chunked); limit transactions to top 10k
    df_h.to_sql("households",    engine, if_exists="replace", index=False,
                method="multi", chunksize=5000)
    df_p.to_sql("products",      engine, if_exists="replace", index=False,
                method="multi", chunksize=5000)
    df_t.head(10_000).to_sql("transactions", engine, if_exists="replace", index=False,
                             method="multi", chunksize=5000)

    print("✅ Data loaded into Azure SQL")

# ─── 4) Run steps ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Step 1: Downloading blobs from Azure Storage…")
    download_blobs()
    print("Step 2–5: Loading into Azure SQL…")
    load_into_sql()
