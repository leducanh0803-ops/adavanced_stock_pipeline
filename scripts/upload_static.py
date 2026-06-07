import boto3
from pathlib import Path

s3 = boto3.client(
    "s3",
    endpoint_url ="http://localhost:9000",
    aws_access_key_id ="minio_admin",
    aws_secret_access_key="minio_password"
)
BUCKET = "lakehouse"

OHLCV_DIR = Path("/mnt/c/DowJones30")

for file in OHLCV_DIR.rglob("*.parquet"):
    ticker = file.stem
    s3_key = f"bronze/ohlcv/1min/{ticker}.parquet"

    print(f"uploading {file.name} -> s3://{BUCKET}/{s3_key}")
    s3.upload_file(str(file),BUCKET,s3_key)

COMPANYFACTS_DIR = Path("/mnt/c/companyfacts")
for json in COMPANYFACTS_DIR.rglob("*.json"):
    s3_key = f"bronze/sec/companyfacts/{json.name}"
    
    print(f"Uploading {json.name} -> s3://{BUCKET}/{s3_key}")
    s3.upload_file(str(json),BUCKET,s3_key)

print("Done.")
