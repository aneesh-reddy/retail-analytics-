#!/usr/bin/env python3
import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import ContainerClient
from sqlalchemy import create_engine

# 1. Load .env
load_dotenv()

AZ_CONN_STR    = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZ_CONTAINER   = os.getenv("AZURE_CONTAINER_NAME", "rawdata")
SQLALCHEMY_URL = os.getenv("AZURE_SQL_CONNECTION_STRING")

def download_blobs():
    client = ContainerClient.from_connection_string(AZ_CONN_STR, container_name=AZ_CONTAINER)
    blobs = client.list_blobs(name_starts_with="8451_The_Complete_Journey_2_Sample-2/")
    for blob in blobs:
        local_path = Path("data/raw") / blob.name
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            data = client.get_blob_client(blob).download_blob().readall()
            f.write(data)
        print(f"Downloaded {blob.name}")

def discover_files():
    files = {
        "household":   list(Path("data/raw").rglob("400_households.csv")),
        "transactions":list(Path("data/raw").rglob("400_transactions.csv")),
        "products":    list(Path("data/raw").rglob("400_products.csv")),
    }
    missing = [k for k,v in files.items() if not v]
    if missing:
        raise FileNotFoundError(f"Missing files for: {missing}")
    return files["household"][0], files["transactions"][0], files["products"][0]

def sanitize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        c.strip().replace(" ", "_")[:128]
        for c in df.columns
    ]
    return df

def load_into_sql():
    engine = create_engine(SQLALCHEMY_URL)
    h_file, t_file, p_file = discover_files()

    df_h = sanitize(pd.read_csv(h_file))
    df_p = sanitize(pd.read_csv(p_file))
    df_t = sanitize(pd.read_csv(
        t_file,
        parse_dates=["PURCHASE_"],  # adjust to your date column name
        nrows=10_000
    ))

    with engine.begin() as conn:
        df_h.to_sql("households",   conn, if_exists="replace", index=False)
        df_p.to_sql("products",     conn, if_exists="replace", index=False)
        df_t.to_sql(
            "transactions",
            conn,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1_000
        )

if __name__ == "__main__":
    download_blobs()
    load_into_sql()
