import os
from google.cloud import bigquery
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_time_series():
    service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")

    client = bigquery.Client.from_service_account_json(service_account_path)

    query = f"""
    SELECT Won_Time, Deal_Value
    FROM `{project_id}.{dataset}.Fact_Deals`
    WHERE Won_Time IS NOT NULL
    ORDER BY Won_Time
    """
    df = client.query(query).to_dataframe()
    df['Won_Time'] = pd.to_datetime(df['Won_Time'])
    return df
