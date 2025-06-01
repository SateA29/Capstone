import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from tasks import upload_to_staging, run_procedure
from datetime import timedelta
from forecast.predict import run_forecasting

def run_etl(ingestion_date):
    # File paths & BigQuery setup
    file_path = f"Daily_Extract/Before_{ingestion_date}.xlsx"
    service_account_path = "capstone-457809-befe1ffb7dfb.json"
    staging_table = "capstone-457809.Capstone_dataset.staging_table"

    client = bigquery.Client.from_service_account_json(service_account_path)

    # Check file
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Load Excel
    df = pd.read_excel(file_path)

    # Parse dates
    for col in ["Deal_Created_Date", "Won_Time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")  # keep datetime format

    # Add metadata columns
    df["staging_raw_id"] = range(1, len(df) + 1)
    df["ingestion_date"] = pd.to_datetime(ingestion_date).date()


    # Upload to staging
    upload_to_staging(client, df, staging_table, ingestion_date)

    # Run procedures
    procedures = [
        "update_dim_owners",
        "update_dim_products",
        "update_dim_organizations",
        "update_dim_dealstatus",
        "update_dim_date",
        "update_fact_deals",
        "update_bridge_table"
    ]

    for proc in procedures:
        run_procedure(client, proc, ingestion_date)

    # Run forecasting logic
    print("Starting forecasting pipeline...")
    forecast_date = datetime.strptime(ingestion_date, "%Y-%m-%d").date() + timedelta(days=1)
    run_forecasting(df, service_account_path)

