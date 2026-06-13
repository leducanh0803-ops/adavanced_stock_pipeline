import boto3
import requests
import pandas as pd
import io
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

HF_API_KEY = os.getenv("HF_API_KEY")
HF_BASE_URL = os.getenv("HF_BASE_URL")
HEADERS = {"X-API-Key": HF_API_KEY}

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY")
BUCKET         = os.getenv("BUCKET")

def load_tickers():
    CSV_PATH = "/opt/airflow/dags/config/tickers.csv"
    return pd.read_csv(CSV_PATH)["ticker"].tolist()

TICKERS = load_tickers()

def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )

def fetch_daily_ohlcv(**context):
    global TICKERS
    execution_date = context["logical_date"]  
    date_str =(execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
    print("=== TASK STARTED ===")

    # date_str = "2026-06-06" 
    print(f"=== Fetching date: {date_str} ===")
    
    s3 = get_s3()
    print("=== S3 client created ===")
    
    # Test S3 connection immediately
    buckets = s3.list_buckets()
    print(f"=== S3 OK, buckets: {[b['Name'] for b in buckets['Buckets']]} ===")
    
    s3 = get_s3()
    success, skipped, failed = 0, 0, 0

    for ticker in TICKERS:
        try:
            resp = requests.get(
                f"{HF_BASE_URL}/bars/{ticker}",
                headers=HEADERS,
                params={
                    "start": date_str,
                    "end": date_str,
                    "format": "parquet",
                },
                timeout=30,
            )
            if resp.status_code == 404:
                print(f"SKIP {ticker} — not found")
                skipped += 1
                continue

            resp.raise_for_status()

            df = pd.read_parquet(io.BytesIO(resp.content))

            if df.empty:
                print(f"SKIP {ticker} — no data for {date_str} (weekend/holiday)")
                skipped += 1
                continue

            df["ticker"] = ticker

            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            s3_key = f"bronze/ohlcv/1min/date={date_str}/{ticker}.parquet"
            s3.put_object(Bucket=BUCKET, Key=s3_key, Body=buffer.getvalue())
            print(f"OK {ticker} → {s3_key} ({len(df)} rows)")
            success += 1

        except Exception as e:
            print(f"FAIL {ticker} — {e}")
            failed += 1

    print(f"\nDone: {success} ok, {skipped} skipped, {failed} failed")

    # Fail the task if too many tickers failed
    if failed > 10:
        raise ValueError(f"Too many failures: {failed}/{len(TICKERS)}")
    
with DAG(
    dag_id="bronze_hf_daily",
    start_date=datetime(2026, 6, 5),
    schedule="0 22 * * 3-7",   # 10pm daily, weekdays only (after US market close)
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=3)},
    tags=["bronze", "hf"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_daily_ohlcv",
        python_callable=fetch_daily_ohlcv,
    )

