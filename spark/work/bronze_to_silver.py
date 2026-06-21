from __future__ import annotations

import os
import re
import boto3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuration Constants
BUCKET = "lakehouse"
BRONZE_PREFIX = "bronze/"
SILVER_NAMESPACE = "nessie.silver"
MANIFEST_TABLE = "bronze_silver_manifest"

# Environment Variables (with fallback defaults)
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


def load_ohlcv(spark: SparkSession, objects: list[dict]) -> DataFrame:
    """
    Bronze ohlcv data ships in two physically different layouts that must be
    normalized onto one common schema before they can land in the same
    Iceberg table:

      bronze/ohlcv/1min/daily_data/date=.../{symbol}.parquet
          timestamp: string : already has its own `date` column

      bronze/ohlcv/1min/date=.../...   (historical backfill)
          timestamp: long, nanoseconds since epoch : no `date` column

    Both already carry a native `symbol` column, so no filename parsing is
    needed to identify the ticker -- that's why there's no ticker-extraction
    step here unlike the original draft of this script.
    """
    daily_keys = [o for o in objects if "/daily_data/" in o["key"]]
    backfill_keys = [o for o in objects if "/daily_data/" not in o["key"]]

    frames: list[DataFrame] = []

    if daily_keys:
        paths = [f"s3a://{BUCKET}/{o['key']}" for o in daily_keys]
        print(f"Reading {len(paths)} daily_data file(s)")
        df_daily = spark.read.parquet(*paths)
        df_daily = df_daily.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
        frames.append(df_daily)

    if backfill_keys:
        paths = [f"s3a://{BUCKET}/{o['key']}" for o in backfill_keys]
        print(f"Reading {len(paths)} backfill file(s)")
        df_backfill = spark.read.parquet(*paths)
        df_backfill = (
            df_backfill
            .withColumn("timestamp", F.timestamp_seconds(F.col("timestamp") / F.lit(1_000_000_000)))
            .withColumn("date", F.to_date(F.col("timestamp")))
        )
        frames.append(df_backfill)

    if not frames:
        raise ValueError("load_ohlcv called with no matching files")

    if len(frames) == 1:
        return frames[0]

    # Both layouts now share an identical column set after normalization;
    # align column order before unioning so unionByName lines up cleanly.
    common_cols = frames[0].columns
    return frames[0].unionByName(frames[1].select(*common_cols))


# --- Per-table schema configuration -------------------------------------
# One entry per distinct bronze "table" (the first folder name under bronze/,
# as derived by table_name_from_key). To onboard a new schema later, add a
# new entry here.
#
#   merge_keys: columns that uniquely identify a row -> enables idempotent
#               upsert via MERGE INTO. Leave empty/omit to fall back to
#               plain append (no dedup protection).
#   loader:     optional custom (spark, objects) -> DataFrame function for
#               tables whose bronze files need special handling (e.g. ohlcv,
#               which spans two physically different layouts). If omitted,
#               falls back to a plain spark.read.parquet(*paths) over all
#               new files for that table.
TABLE_CONFIG: dict[str, dict] = {
    "ohlcv": {
        "merge_keys": ["symbol", "timestamp"],  # grain is one row per minute bar
        "loader": load_ohlcv,
    },
    "economic_calendar": {
        # API schema documents `id` (bigint) as the primary key.
        "merge_keys": ["id"],
    },
}


def get_spark() -> SparkSession:
    """Initialize or retrieve the active Spark Session."""
    return SparkSession.builder.appName("bronze_to_silver_incremental").getOrCreate()


def list_bronze_parquet_files(s3_client) -> list[dict]:
    """Recursively list all .parquet objects under bronze/."""
    objects = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=BRONZE_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                objects.append({"key": obj["Key"], "size": obj["Size"]})
    return objects


def table_name_from_key(key: str) -> str:
    """
    Extracts the table name from the first path segment under bronze/.
    e.g. bronze/ohlcv/1min/daily_data/date=2026-06-18/MMM.parquet -> ohlcv
    e.g. bronze/ohlcv/1min/date=2026-06-19/MMM.parquet            -> ohlcv
    e.g. bronze/economic_calendar/date=2026-06-18/batch.parquet   -> economic_calendar
    """
    rel = key[len(BRONZE_PREFIX):]
    parts = rel.split("/")
    if len(parts) == 1:
        name = os.path.splitext(parts[0])[0]
    else:
        name = parts[0]

    name = name.lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def group_files_by_table(objects: list[dict]) -> dict[str, list[dict]]:
    """Groups S3 objects by their calculated target table names."""
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
    """Append rows to the manifest table for files just processed."""
    if not objects:
        return
    rows = [(table_name, obj["key"], obj["size"], "loaded") for obj in objects]
    df = spark.createDataFrame(rows, schema=["table_name", "file_key", "file_size", "status"])
    df = df.coalesce(4)
    df.write.jdbc(
        url=PG_JDBC_URL,
        table=MANIFEST_TABLE,
        mode="append",
        properties=PG_PROPERTIES,
    )


def merge_into_silver(spark: SparkSession, df: DataFrame, full_table_name: str, table_short_name: str) -> None:
    """Performs an idempotent MERGE INTO or APPEND into the Iceberg Silver table."""
    config = TABLE_CONFIG.get(table_short_name, {})
    merge_keys = config.get("merge_keys")

    # If Iceberg table doesn't exist yet, initialize it directly with the schema
    if not spark.catalog.tableExists(full_table_name):
        print(f"Creating new Iceberg table: {full_table_name}")
        df.writeTo(full_table_name).using("iceberg").create()
        return

    # Fallback to append if no merge keys are configured
    if not merge_keys:
        print(f"No merge keys configured for '{table_short_name}', appending rows directly.")
        df.writeTo(full_table_name).append()
        return

    # --- Deduplication Safeguard ---
    # Prevents Spark from crashing if the same incoming batch contains duplicate rows for a key
    print(f"Safeguarding batch: Removing row duplicates on keys {merge_keys}")
    window_spec = Window.partitionBy(*merge_keys).orderBy(
        F.col("timestamp").desc() if "timestamp" in df.columns else F.lit(1)
    )
    df_deduped = df.withColumn("_row_num", F.row_number().over(window_spec)) \
                   .filter(F.col("_row_num") == 1) \
                   .drop("_row_num")

    print(f"Upserting into {full_table_name} on keys {merge_keys}")
    staging_view = f"staging_{table_short_name}"
    df_deduped.createOrReplaceTempView(staging_view)

    # Dynamic SQL Building block generations
    on_clause = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
    all_cols = df_deduped.columns
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

    # Initialize standard boto3 client to communicate with MinIO S3
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

    # Group files by destination table logic
    groups = group_files_by_table(new_objects)

    for table, objects in groups.items():
        config = TABLE_CONFIG.get(table, {})
        full_table_name = f"{SILVER_NAMESPACE}.{table}"
        loader = config.get("loader")

        if loader:
            df = loader(spark, objects)
        else:
            paths = [f"s3a://{BUCKET}/{o['key']}" for o in objects]
            print(f"\nReading {len(paths)} new file(s) for table '{full_table_name}'")
            df = spark.read.parquet(*paths)

        # Perform the safe merge transaction
        merge_into_silver(spark, df, full_table_name, table)

        row_count = df.count()
        print(f"Processed {row_count} total row(s) into {full_table_name}")

        # Update the state manifest database table
        record_loaded_files(spark, table, objects)
        print(f"Recorded {len(objects)} file(s) in manifest for '{table}'")

    spark.stop()


if __name__ == "__main__":
    main()