import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get project and dataset from .env
project_id = os.getenv("GCP_PROJECT_ID")
dataset = os.getenv("BQ_DATASET")

def update_forecast_in_bigquery(service_account_path, date, value, model):
    client = bigquery.Client.from_service_account_json(service_account_path)

    sql = f"""
    UPDATE `{project_id}.{dataset}.Fact_Deals`
    SET
      Predicted_Deal_Value = {value},
      Predicted_Won_Time = DATE('{date.strftime('%Y-%m-%d')}'),
      Prediction_Model = '{model}',
      Prediction_Timestamp = CURRENT_TIMESTAMP()
    WHERE Deal_ID_SK_PK = -2
    """
    client.query(sql).result()
    print(f"Forecast for {date.date()} updated using {model}: {value:.2f}")
