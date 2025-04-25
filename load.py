# load.py
import os
from dotenv import load_dotenv
from azure.storage.blob import ContainerClient
import pandas as pd
from sqlalchemy import create_engine

# ─── CONFIG ─────────────────────────────────────────────────────
load_dotenv()  # expects .env next to this file

AZ_CONN   = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "rawdata")
SQL_CONN  = os.getenv("AZURE_SQL_CONNECTION_STRING")

RAW_DIR   = "data/raw/8451_The_Complete_Journey_2_Sample-2"
# ─────────────────────────────────────────────────────────────────

def download_blobs():
    client = ContainerClient.from_connection_string(AZ_CONN, container_name=CONTAINER)
    os.makedirs(RAW_DIR, exist_ok=True)
    print("▶️ Downloading blobs…")
    for blob in client.list_blobs(name_starts_with="8451_The_Complete_Journey_2_Sample-2/"):
        local_path = os.path.join("data/raw", blob.name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(client.download_blob(blob.name).readall())
        print("  •", blob.name)

def discover_files():
    h = os.path.join(RAW_DIR, "400_households.csv")
    t = os.path.join(RAW_DIR, "400_transactions.csv")
    p = os.path.join(RAW_DIR, "400_products.csv")
    miss = [x for x in (h, t, p) if not os.path.exists(x)]
    if miss:
        raise FileNotFoundError(f"Missing files: {miss}")
    return h, t, p

def sanitize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    for c in df.select_dtypes("object"):
        df[c] = df[c].str.strip()
    return df

def load_into_sql():
    download_blobs()
    hfile, tfile, pfile = discover_files()

    print("▶️ Reading & sanitizing CSVs…")
    df_h = sanitize(pd.read_csv(hfile))
    df_p = sanitize(pd.read_csv(pfile))
    df_t = sanitize(pd.read_csv(
        tfile,
        parse_dates=["PURCHASE_"],
        nrows=10_000
    )).rename(columns={"PURCHASE_": "PURCHASE_DATE"})

    print("▶️ Loading into Azure SQL…")
    engine = create_engine(SQL_CONN)
    with engine.begin() as conn:
        df_h.to_sql(
            "households", conn,
            if_exists="replace", index=False,
            method="multi", chunksize=5_000
        )
        df_p.to_sql(
            "products", conn,
            if_exists="replace", index=False,
            method="multi", chunksize=5_000
        )
        df_t.to_sql(
            "transactions", conn,
            if_exists="replace", index=False,
            method="multi", chunksize=5_000
        )

    print("✅ All tables loaded successfully.")

if __name__ == "__main__":
    load_into_sql()
