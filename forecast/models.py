import pandas as pd
import numpy as np
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from prophet import Prophet
from xgboost import XGBRegressor

def prepare_series(df):
    df = df[df['Won_Time'].notna() & (df['Deal_Value'] > 0)]

    df['Won_Time'] = pd.to_datetime(df['Won_Time'], errors='coerce')

    if df.empty:
        print("No data left after filtering out null Won_Time or zero Deal_Value.")
        return pd.DataFrame()

    ts = df.groupby(df['Won_Time'].dt.date)['Deal_Value'].sum().reset_index()
    ts.columns = ['ds', 'y']
    ts['ds'] = pd.to_datetime(ts['ds'])
    ts = ts.set_index('ds').asfreq('D').fillna(0)
    ts['y'] = ts['y'].astype(float)

    if ts['y'].sum() == 0:
        print("All deal values are zero. Forecasting results may not be meaningful.")

    return ts

def evaluate_models(ts, forecast_days=1):
    if ts.empty or ts['y'].sum() == 0:
        print("No valid time series data available. Skipping forecasting.")
        return {}, ts

    results = {}

    # Prophet
    try:
        prophet_df = ts.reset_index().rename(columns={"ds": "ds", "y": "y"})
        prophet = Prophet(daily_seasonality=True)
        prophet.fit(prophet_df)
        future = prophet.make_future_dataframe(periods=forecast_days)
        forecast = prophet.predict(future)
        forecast_future = forecast[forecast['ds'] > ts.index.max()]
        prophet_pred = forecast_future.set_index('ds')['yhat'][:forecast_days].clip(lower=0)
        results['Prophet'] = (prophet, prophet_pred)
    except Exception as e:
        print(f"Prophet failed: {e}")

    # ETS
    try:
        ets_model = ExponentialSmoothing(ts['y'], trend='add', seasonal='add', seasonal_periods=7)
        ets_fit = ets_model.fit()
        ets_pred = ets_fit.forecast(steps=forecast_days).clip(lower=0)
        forecast_index = pd.date_range(start=ts.index.max() + pd.Timedelta(days=1), periods=forecast_days)
        results['ETS'] = (ets_fit, pd.Series(ets_pred, index=forecast_index))
    except Exception as e:
        print(f"ETS failed: {e}")

    # XGBoost
    try:
        df_xgb = ts.reset_index()
        df_xgb['day'] = df_xgb['ds'].dt.day
        df_xgb['month'] = df_xgb['ds'].dt.month
        df_xgb['weekday'] = df_xgb['ds'].dt.weekday
        features = ['day', 'month', 'weekday']

        X = df_xgb[features]
        y = df_xgb['y']

        model = XGBRegressor(n_estimators=100)
        model.fit(X, y)

        next_day = ts.index.max() + pd.Timedelta(days=1)
        next_features = pd.DataFrame([{
            'day': next_day.day,
            'month': next_day.month,
            'weekday': next_day.weekday()
        }])

        pred = np.clip(model.predict(next_features), 0, None)
        results['XGBoost'] = (model, pd.Series(pred, index=[next_day]))
    except Exception as e:
        print(f"XGBoost failed: {e}")

    return results, ts
