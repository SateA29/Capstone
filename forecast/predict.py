import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from forecast.models import prepare_series, evaluate_models
from datetime import timedelta

# Load environment variables
load_dotenv()

# Get values from .env
service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
project_id = os.getenv("GCP_PROJECT_ID")
dataset = os.getenv("BQ_DATASET")

def update_forecast_in_bigquery(service_account_path, forecast_date, forecast_value, model_name):
    client = bigquery.Client.from_service_account_json(service_account_path)

    sql = f"""
    UPDATE `{project_id}.{dataset}.Fact_Deals`
    SET
      Predicted_Deal_Value = {forecast_value},
      Predicted_Won_Time = DATE('{forecast_date.strftime('%Y-%m-%d')}'),
      Prediction_Model = '{model_name}',
      Prediction_Timestamp = CURRENT_TIMESTAMP()
    WHERE Deal_ID_SK_PK = -2
    """
    client.query(sql).result()
    print(f"Forecast for {forecast_date.date()} updated using {model_name}: {forecast_value:.2f}")

def run_forecasting(df_today, service_account_path):
    client = bigquery.Client.from_service_account_json(service_account_path)

    query = f"""
        SELECT Won_Time, Deal_Value
        FROM `{project_id}.{dataset}.Fact_Deals`
        WHERE Won_Time IS NOT NULL AND Deal_Value > 0
    """
    df_history = client.query(query).to_dataframe()

    # Filter today's data and append
    df_today_clean = df_today[df_today['Won_Time'].notna() & (df_today['Deal_Value'] > 0)]
    df_combined = pd.concat([df_history, df_today_clean], ignore_index=True)

    # Prepare time series and forecast
    ts = prepare_series(df_combined)
    results, ts = evaluate_models(ts, forecast_days=1)

    if not results:
        print("All models failed. No forecast produced.")
        return

    best_model_name = list(results.keys())[0]
    best_model, best_forecast = results[best_model_name]

    forecast_date = pd.to_datetime(df_today['ingestion_date'].max()) + timedelta(days=1)
    forecast_value = best_forecast.iloc[0]

    update_forecast_in_bigquery(service_account_path, forecast_date, forecast_value, best_model_name)
