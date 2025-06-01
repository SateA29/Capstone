import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get dataset from .env
project_id = os.getenv("GCP_PROJECT_ID")
dataset_name = os.getenv("BQ_DATASET")

def upload_to_staging(client, df, staging_table, ingestion_date):
    print(f"Uploading {len(df)} rows for ingestion_date = {ingestion_date}...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, staging_table, job_config=job_config).result()
    print("Uploaded to staging.")

def run_procedure(client, procedure_name, ingestion_date):
    full_procedure_name = f"`{project_id}.{dataset_name}.{procedure_name}`"
    query = f"""
        CALL {full_procedure_name}(DATE '{ingestion_date}');
    """
    print(f"Running procedure: {procedure_name}...")
    client.query(query).result()
    print(f"Procedure {procedure_name} executed successfully.")
