import argparse
from flow import run_etl

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Ingestion date (YYYY-MM-DD)")
    args = parser.parse_args()

    run_etl(args.date)
