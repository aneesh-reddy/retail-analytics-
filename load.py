# load.py
import os
from dotenv import load_dotenv
from azure.storage.blob import ContainerClient
import pandas as pd
from sqlalchemy import create_engine

# ─── CONFIG ─────────────────────────────────────────────────────
load_dotenv()  # Load environment variables from .env file

AZ_CONN   = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "rawdata")
SQL_CONN  = os.getenv("AZURE_SQL_CONNECTION_STRING")
RAW_DIR   = "data/raw/8451_The_Complete_Journey_2_Sample-2"
# ─────────────────────────────────────────────────────────────────

def download_blobs():
    """
    Download CSV blobs from Azure Blob Storage into the local RAW_DIR.
    """
    client = ContainerClient.from_connection_string(AZ_CONN, container_name=CONTAINER)
    os.makedirs(RAW_DIR, exist_ok=True)
    print("▶️ Downloading blobs…")
    for blob in client.list_blobs(name_starts_with=os.path.basename(RAW_DIR) + "/"):
        local_path = os.path.join("data/raw", blob.name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            data = client.download_blob(blob.name).readall()
            f.write(data)
        print(f"  • {blob.name}")


def discover_files():
    """
    Ensure that all required CSV files are present locally.
    """
    files = {
        'households': os.path.join(RAW_DIR, "400_households.csv"),
        'transactions': os.path.join(RAW_DIR, "400_transactions.csv"),
        'products': os.path.join(RAW_DIR, "400_products.csv")
    }
    missing = [name for name, path in files.items() if not os.path.exists(path)]
    if missing:
        raise FileNotFoundError(f"Missing files: {missing}")
    return files['households'], files['transactions'], files['products']


def sanitize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trim whitespace from column names and string values.
    """
    df = df.copy()
    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].str.strip()
    return df


def load_into_sql():
    """
    Main function to download blobs, load into DataFrames, and write to Azure SQL.
    """
    download_blobs()
    hfile, tfile, pfile = discover_files()

    print("▶️ Reading & sanitizing CSVs…")
    df_h = sanitize(pd.read_csv(hfile))
    df_p = sanitize(pd.read_csv(pfile))
    df_t = sanitize(
        pd.read_csv(
            tfile,
            parse_dates=["PURCHASE_"],
            nrows=10000
        )
    ).rename(columns={"PURCHASE_": "PURCHASE_DATE"})

    print("▶️ Loading into Azure SQL…")
    # Create SQLAlchemy engine; ensure SQL_CONN is a valid SQLAlchemy URL (e.g., mssql+pymssql://...)
    engine = create_engine(SQL_CONN)

    with engine.begin() as conn:
        # Write each DataFrame to its respective table
        df_h.to_sql(
            "households",
            conn,
            if_exists="replace",
            index=False,
            chunksize=5000
        )
        df_p.to_sql(
            "products",
            conn,
            if_exists="replace",
            index=False,
            chunksize=5000
        )
        df_t.to_sql(
            "transactions",
            conn,
            if_exists="replace",
            index=False,
            chunksize=5000
        )

    print("✅ All tables loaded successfully.")


if __name__ == "__main__":
    load_into_sql()
