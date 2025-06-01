import os
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

from tasks import upload_to_staging, run_procedure
from forecast.predict import run_forecasting

from dotenv import load_dotenv
load_dotenv()

def run_etl(ingestion_date):
    # Load values from .env
    service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    bq_dataset = os.getenv("BQ_DATASET")
    staging_table_name = os.getenv("STAGING_TABLE")
    excel_folder = os.getenv("EXCEL_FOLDER")

    # Construct paths
    file_path = f"{excel_folder}/deals_{ingestion_date}.xlsx"
    staging_table = f"{bq_dataset}.{staging_table_name}"

    # Initialize BigQuery client
    client = bigquery.Client.from_service_account_json(service_account_path)

    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Load Excel
    df = pd.read_excel(file_path)

    # Parse datetime columns
    for col in ["Deal_Created_Date", "Won_Time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

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

    # Run forecasting
    print("Starting forecasting pipeline...")
    forecast_date = datetime.strptime(ingestion_date, "%Y-%m-%d").date() + timedelta(days=1)
    run_forecasting(df, service_account_path)
