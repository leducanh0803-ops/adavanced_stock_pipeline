import boto3
import requests
import pandas as pd
import io
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
HF_API_KEY     = os.getenv("HF_API_KEY")
HF_BASE_URL    = "https://api.hfdatalibrary.com/v1"
HEADERS        = {"X-API-Key": HF_API_KEY}
MINIO_ENDPOINT = "http://localhost:9000"   # localhost for outside-Docker testing
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY")
BUCKET         = "lakehouse"
DATE           = "2026-06-06"
TICKERS        = ["AAPL", "MSFT", "NVDA"]

# ── S3 client ─────────────────────────────────────────────────────
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
)

# ── Sanity check ──────────────────────────────────────────────────
print("=== Checking MinIO connection ===")
buckets = s3.list_buckets()
print(f"Buckets: {[b['Name'] for b in buckets['Buckets']]}")

print(f"\n=== Fetching date: {DATE} ===")

success, skipped, failed = 0, 0, 0

for ticker in TICKERS:
    print(f"\n── {ticker} ──")
    try:
        # ── API request ───────────────────────────────────────────
        resp = requests.get(
            f"{HF_BASE_URL}/bars/{ticker}",
            headers=HEADERS,
            params={"start": DATE, "end": DATE, "format": "parquet"},
            timeout=30,
        )
        print(f"Status: {resp.status_code}")

        if resp.status_code == 404:
            print(f"SKIP — not found in API")
            skipped += 1
            continue

        resp.raise_for_status()

        # ── Parse response ────────────────────────────────────────
        df = pd.read_parquet(io.BytesIO(resp.content))
        print(f"Rows: {len(df)}")
        print(f"Columns: {df.columns.tolist()}")
        print(df.head(2))

        if df.empty:
            print(f"SKIP — empty dataframe")
            skipped += 1
            continue

        df["ticker"] = ticker

        # ── Upload to MinIO ───────────────────────────────────────
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3_key = f"bronze/ohlcv/1min/date={DATE}/{ticker}.parquet"
        s3.put_object(Bucket=BUCKET, Key=s3_key, Body=buffer.getvalue())
        print(f"Uploaded → s3://{BUCKET}/{s3_key}")
        success += 1

    except Exception as e:
        print(f"FAIL — {e}")
        failed += 1

# ── Summary ───────────────────────────────────────────────────────
print(f"\n=== Done: {success} ok | {skipped} skipped | {failed} failed ===")

# ── Verify files landed in MinIO ──────────────────────────────────
print(f"\n=== Verifying MinIO contents ===")
result = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"bronze/ohlcv/1min/date={DATE}/")
contents = result.get("Contents", [])
print(f"Files in MinIO for {DATE}: {len(contents)}")
for obj in contents:
    print(f"  {obj['Key']}  ({obj['Size']} bytes)")