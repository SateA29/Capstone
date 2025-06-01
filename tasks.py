from google.cloud import bigquery

def upload_to_staging(client, df, staging_table, ingestion_date):
    print(f"Uploading {len(df)} rows for ingestion_date = {ingestion_date}...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, staging_table, job_config=job_config).result()
    print("Uploaded to staging.")

def run_procedure(client, procedure_name, ingestion_date):
    query = f"""
        CALL `capstone-457809.Capstone_dataset.{procedure_name}`(DATE '{ingestion_date}');
    """
    print(f"Running procedure: {procedure_name}...")
    client.query(query).result()
    print(f"Procedure {procedure_name} executed successfully.")
