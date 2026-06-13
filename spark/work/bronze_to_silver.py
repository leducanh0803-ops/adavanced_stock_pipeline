"""
Bronze -> Silver (incremental, manifest-tracked)

Scans s3a://lakehouse/bronze/ for .parquet files (recursive), skips files
already recorded in the `bronze_silver_manifest` Postgres table, reads the
new files, and MERGE INTOs them into the corresponding Iceberg table in
the `nessie.silver` namespace (upsert on a configurable key, fallback to
append if no key columns are present).

Run inside the spark container, e.g.:
    spark-submit \
        --jars /opt/spark/jars/postgresql-42.7.3.jar \
        /opt/spark/work/bronze_to_silver.py

Required env vars (already present in your compose for af / spark containers):
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, MINIO_ENDPOINT
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
import boto3
import os
import re

BUCKET = "lakehouse"
BRONZE_PREFIX = "bronze/"
SILVER_NAMESPACE = "nessie.silver"
MANIFEST_TABLE = "bronze_silver_manifest"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minio_admin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio_password")

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "db")
PG_USER = os.environ.get("PG_USER", "db_user")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "db_password")

PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# Per-table merge keys for upsert. If a table isn't listed here, new rows
# are simply appended (no dedup).
MERGE_KEYS: dict[str, list[str]] = {
    "ohlcv": ["ticker", "date"],
    # "fundamentals": ["ticker", "report_date"],
}


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("bronze_to_silver_incremental").getOrCreate()


def list_bronze_parquet_files(s3_client) -> list[dict]:
    """Recursively list all .parquet objects under bronze/ (rglob equivalent)."""
    objects = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=BRONZE_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                objects.append({"key": obj["Key"], "size": obj["Size"]})
    return objects


def table_name_from_key(key: str) -> str:
    """
    Use the first path segment under bronze/ as the table name, so all
    symbols/dates for a dataset land in one table.

    e.g. bronze/ohlcv/AAPL/2024-01-01.parquet      -> ohlcv
         bronze/fundamentals/AAPL/2024-Q1.parquet  -> fundamentals
         bronze/news/2024-01-01.parquet            -> news
    """
    rel = key[len(BRONZE_PREFIX):]
    parts = rel.split("/")
    if len(parts) == 1:
        # file sits directly under bronze/, use filename (without extension)
        name = os.path.splitext(parts[0])[0]
    else:
        name = parts[0]

    name = name.lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def group_files_by_table(objects: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for obj in objects:
        table = table_name_from_key(obj["key"])
        groups.setdefault(table, []).append(obj)
    return groups


def get_loaded_keys(spark: SparkSession) -> set[str]:
    """Read already-loaded file keys from the Postgres manifest table."""
    try:
        df = spark.read.jdbc(
            url=PG_JDBC_URL,
            table=f"(SELECT file_key FROM {MANIFEST_TABLE} WHERE status = 'loaded') AS t",
            properties=PG_PROPERTIES,
        )
        return {row["file_key"] for row in df.collect()}
    except Exception as e:
        print(f"Could not read manifest table (assuming empty): {e}")
        return set()


def record_loaded_files(spark: SparkSession, table_name: str, objects: list[dict]) -> None:
    """Append rows to the manifest table for files just loaded."""
    if not objects:
        return
    rows = [(table_name, obj["key"], obj["size"], "loaded") for obj in objects]
    df = spark.createDataFrame(rows, schema=["table_name", "file_key", "file_size", "status"])
    df.write.jdbc(
        url=PG_JDBC_URL,
        table=MANIFEST_TABLE,
        mode="append",
        properties=PG_PROPERTIES,
    )


def merge_into_silver(spark: SparkSession, df: DataFrame, full_table_name: str, table_short_name: str) -> None:
    merge_keys = MERGE_KEYS.get(table_short_name)

    if not spark.catalog.tableExists(full_table_name):
        print(f"Creating new table {full_table_name}")
        df.writeTo(full_table_name).using("iceberg").create()
        return

    if not merge_keys:
        print(f"No merge keys configured for '{table_short_name}', appending rows to {full_table_name}")
        df.writeTo(full_table_name).append()
        return

    print(f"Upserting into {full_table_name} on keys {merge_keys}")
    staging_view = f"staging_{table_short_name}"
    df.createOrReplaceTempView(staging_view)

    on_clause = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
    all_cols = df.columns
    update_set = ", ".join(f"target.{c} = source.{c}" for c in all_cols)
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join(f"source.{c}" for c in all_cols)

    merge_sql = f"""
        MERGE INTO {full_table_name} AS target
        USING {staging_view} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    spark.sql(merge_sql)


def main():
    spark = get_spark()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    all_objects = list_bronze_parquet_files(s3)
    if not all_objects:
        print(f"No parquet files found under s3a://{BUCKET}/{BRONZE_PREFIX}")
        return

    loaded_keys = get_loaded_keys(spark)
    new_objects = [o for o in all_objects if o["key"] not in loaded_keys]

    print(f"Found {len(all_objects)} total file(s), {len(loaded_keys)} already loaded, "
          f"{len(new_objects)} new file(s) to process.")

    if not new_objects:
        print("Nothing new to load.")
        spark.stop()
        return

    groups = group_files_by_table(new_objects)

    for table, objects in groups.items():
        paths = [f"s3a://{BUCKET}/{o['key']}" for o in objects]
        full_table_name = f"{SILVER_NAMESPACE}.{table}"

        print(f"\nReading {len(paths)} new file(s) for table '{full_table_name}'")
        df = spark.read.parquet(*paths)

        merge_into_silver(spark, df, full_table_name, table)

        row_count = df.count()
        print(f"Processed {row_count} row(s) for {full_table_name}")

        record_loaded_files(spark, table, objects)
        print(f"Recorded {len(objects)} file(s) in manifest for '{table}'")

    spark.stop()


if __name__ == "__main__":
    main()