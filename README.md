# Student: Sate Antaranyan 
# Capstone: Developing a Sales Analysis and Forecasting Framework for a Sales Outsourcing Company

This project semi-automates the ingestion, transformation, and forecasting of deal data using Google BigQuery and Python.

---

## Project Overview

- Loads Excel files with deal data into a staging table in BigQuery
- Updates all dimensional and fact tables using stored procedures
- Predicts next-day deal value using Prophet, ETS, and XGBoost
- Updates predictions back into the Fact_Deals table

---

## Project Structure

```
Capstone2025/
├── client_secrets/                 # Contains GCP service account JSON (not committed)
│   └── your-key.json
├── Daily_Extract/                 # Excel data files (deals_YYYY-MM-DD.xlsx)
├── .env.example                   # Template for setup
|── .env                           # Local credentials and configs
├── main.py                        # Entry point
├── flow.py                        # Orchestrates ETL
├── forecast/
│   ├── models.py                  # Forecasting models
│   └── predict.py                 # Runs prediction pipeline
├── tasks.py                       # Uploads to staging & calls procedures
├── queries/                       # SQL scripts for setup
│   ├── Create_Table.sql
│   ├── Primary_Key.sql
│   ├── Foreign_Key.sql
│   └── Update.sql
├── requirements.txt
```

---

## Setup Instructions

### ✅ 1. Environment Setup

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

### ✅ 2. Set Up BigQuery Project and Dataset (One-time setup)

1. Go to [https://console.cloud.google.com](https://console.cloud.google.com)
2. **Create a new GCP project** or select an existing one.
3. Enable the **BigQuery API** from the "APIs & Services" section.
4. Navigate to **BigQuery** from the left sidebar.
5. Click your project name and then **"Create Dataset"**.
6. Name your dataset (e.g., `Demo_Dataset`) and choose:
   - Location: `US` (or your preferred location)
   - Leave other options as default
7. Click **"Create Dataset"**.

---

### ✅ 3. Configure Credentials

1. In GCP Console, go to:
   - IAM & Admin → Service Accounts → Create Service Account
2. Grant it BigQuery Admin role and download the **JSON key file**.
3. Move the downloaded JSON into your project:
   ```
   client_secrets/your-key.json
   ```
4. Fill in `.env.example`
5. Copy the contents of `.env.example` to `.env`
6. `.env` should look similar to this:
   ```env
   GOOGLE_APPLICATION_CREDENTIALS=client_secrets/your-key.json
   GCP_PROJECT_ID=your-gcp-project-id
   BQ_DATASET=your-dataset-name
   ```

---

### ✅ 4. Prepare BigQuery Tables and Procedures

Before running the pipeline:

1. Open the `queries/Create_Table.sql` file, copy its contents, paste into the BigQuery editor, and run it.
2. Open the `queries/Primary_Key.sql` file, copy its contents, paste into BigQuery, and run it.
3. Open the `queries/Foreign_Key.sql` file, copy its contents, paste into BigQuery, and run it.
4. Open the `queries/Update.sql` file, copy its contents and paste into BigQuery — **before running it**:
   - Replace every instance of `GCP_PROJECT_ID.BQ_DATASET.` with your actual project ID and dataset name (e.g., `capstone-457809.Demo_Dataset.`)

---

## Run the Pipeline

Run the project with a specific date:

```bash
python main.py --date=2025-02-10
```

This will:
- Load the Excel file `Daily_Extract/deals_2025-02-10.xlsx`
- Upload it to BigQuery
- Run all update procedures
- Run forecasting models
- Store the prediction in `Fact_Deals`

> The sample Excel file **must be named** in this format:
> ```
> deals_YYYY-MM-DD.xlsx
> ```
> and placed inside the `Daily_Extract/` folder.

---

## For Reviewers

1. Clone or unzip the project.
2. Move `client_secrets/` into the root folder.
3. Create `.env` from `.env.example` and update it.
4. Run `pip install -r requirements.txt`
5. Ensure that the sample data file is named as `deals_YYYY-MM-DD.xlsx` and located in `Daily_Extract/`
6. Run the pipeline with a sample date:
   ```bash
   python main.py --date=YYYY-MM-DD
   ```

---
