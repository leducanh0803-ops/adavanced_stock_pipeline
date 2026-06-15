from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
import os

SILVER_NAMESPACE = "nessie.silver"
GOLD_NAMESPACE   = "nessie.gold"
SILVER_OHLCV     = f"{SILVER_NAMESPACE}.ohlcv"
GOLD_OHLCV_DAILY = f"{GOLD_NAMESPACE}.ohlcv_daily"
NANOS_PER_SEC    = 1_000_000_000


# ── Spark session ──────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    return SparkSession.builder.appName("silver_to_gold").getOrCreate()


# ── Window shortcuts ───────────────────────────────────────────────────────────

def pw(n: int) -> Window:
    """Rolling window of n days partitioned by ticker, ordered by date."""
    return Window.partitionBy("ticker").orderBy("date").rowsBetween(-n, 0)


def ew() -> Window:
    """Expanding (cumulative) window partitioned by ticker, ordered by date."""
    return Window.partitionBy("ticker").orderBy("date").rowsBetween(
        Window.unboundedPreceding, 0
    )


def rw() -> Window:
    """Row-level lag/lead window partitioned by ticker."""
    return Window.partitionBy("ticker").orderBy("date")


def cw() -> Window:
    """Cross-sectional window: all tickers on the same date."""
    return Window.partitionBy("date")


# ── Write helper ───────────────────────────────────────────────────────────────

def write_iceberg(spark: SparkSession, df: DataFrame, table: str) -> None:
    if spark.catalog.tableExists(table):
        df.writeTo(table).overwritePartitions()
    else:
        df.writeTo(table).using("iceberg").create()
    count = df.count()
    print(f"   ✓ {table}: {count:,} rows")


# ── Trend indicators ───────────────────────────────────────────────────────────

def add_trend(df: DataFrame) -> DataFrame:
    # SMA
    for p in [10, 20, 50, 100, 200]:
        df = df.withColumn(f"sma_{p}", F.avg("close").over(pw(p)).cast(DoubleType()))

    # EMA (approximated via tight SMA — acceptable for daily TF)
    for p in [9, 12, 21, 26, 50]:
        df = df.withColumn(f"ema_{p}", F.avg("close").over(pw(p)).cast(DoubleType()))

    # DEMA = 2*EMA(n) - EMA(EMA(n))  — double EMA (reduces lag)
    df = df.withColumn("_dema_inner", F.avg("close").over(pw(20)))
    df = df.withColumn("dema_20",
        (2 * F.col("ema_20") - F.avg("_dema_inner").over(pw(20))).cast(DoubleType())
    ).drop("_dema_inner")

    # TEMA = 3*EMA - 3*EMA(EMA) + EMA(EMA(EMA))
    df = df.withColumn("_tema_e1", F.avg("close").over(pw(20)))
    df = df.withColumn("_tema_e2", F.avg("_tema_e1").over(pw(20)))
    df = df.withColumn("_tema_e3", F.avg("_tema_e2").over(pw(20)))
    df = df.withColumn("tema_20",
        (3*F.col("_tema_e1") - 3*F.col("_tema_e2") + F.col("_tema_e3")).cast(DoubleType())
    ).drop("_tema_e1", "_tema_e2", "_tema_e3")

    # HMA = WMA(2*WMA(n/2) - WMA(n), sqrt(n))  — Hull MA, very low lag
    # Approximated: SMA(sqrt(20)) of (2*SMA(10) - SMA(20))
    df = df.withColumn("_hma_inner",
        (2 * F.avg("close").over(pw(10)) - F.avg("close").over(pw(20)))
    )
    df = df.withColumn("hma_20",
        F.avg("_hma_inner").over(pw(4)).cast(DoubleType())  # sqrt(20) ≈ 4
    ).drop("_hma_inner")

    # Ichimoku Cloud (daily data; standard params: 9/26/52)
    df = df.withColumn("ichimoku_tenkan",  # (9-day high + 9-day low) / 2
        ((F.max("high").over(pw(9)) + F.min("low").over(pw(9))) / 2).cast(DoubleType())
    )
    df = df.withColumn("ichimoku_kijun",   # (26-day high + 26-day low) / 2
        ((F.max("high").over(pw(26)) + F.min("low").over(pw(26))) / 2).cast(DoubleType())
    )
    df = df.withColumn("ichimoku_senkou_a", # (tenkan + kijun) / 2
        ((F.col("ichimoku_tenkan") + F.col("ichimoku_kijun")) / 2).cast(DoubleType())
    )
    df = df.withColumn("ichimoku_senkou_b", # (52-day high + 52-day low) / 2
        ((F.max("high").over(pw(52)) + F.min("low").over(pw(52))) / 2).cast(DoubleType())
    )
    df = df.withColumn("ichimoku_chikou",   # close shifted back 26 periods
        F.lag("close", 26).over(rw()).cast(DoubleType())
    )
    df = df.withColumn("ichimoku_cloud_top",
        F.greatest("ichimoku_senkou_a", "ichimoku_senkou_b").cast(DoubleType())
    )
    df = df.withColumn("ichimoku_cloud_bottom",
        F.least("ichimoku_senkou_a", "ichimoku_senkou_b").cast(DoubleType())
    )

    # Price vs key MAs (ratio — useful as ML features)
    for p in [20, 50, 200]:
        df = df.withColumn(f"close_vs_sma{p}",
            (F.col("close") / F.col(f"sma_{p}") - 1).cast(DoubleType())
        )

    return df


# ── Momentum indicators ────────────────────────────────────────────────────────

def add_momentum(df: DataFrame) -> DataFrame:
    # MACD (EMA12 - EMA26), signal EMA9 of MACD, histogram
    df = df.withColumn("macd", (F.col("ema_12") - F.col("ema_26")).cast(DoubleType()))
    df = df.withColumn("macd_signal", F.avg("macd").over(pw(9)).cast(DoubleType()))
    df = df.withColumn("macd_hist", (F.col("macd") - F.col("macd_signal")).cast(DoubleType()))
    df = df.withColumn("macd_hist_prev", F.lag("macd_hist", 1).over(rw()).cast(DoubleType()))
    df = df.withColumn("macd_crossover",  # 1 = bullish cross, -1 = bearish cross, 0 = none
        F.when((F.col("macd_hist") > 0) & (F.col("macd_hist_prev") <= 0), F.lit(1))
         .when((F.col("macd_hist") < 0) & (F.col("macd_hist_prev") >= 0), F.lit(-1))
         .otherwise(F.lit(0)).cast(IntegerType())
    ).drop("macd_hist_prev")

    # RSI (7, 14, 21)
    for period in [7, 14, 21]:
        df = df.withColumn("_delta", F.col("daily_return_pct"))
        df = df.withColumn("_gain", F.when(F.col("_delta") > 0, F.col("_delta")).otherwise(0.0))
        df = df.withColumn("_loss", F.when(F.col("_delta") < 0, -F.col("_delta")).otherwise(0.0))
        df = df.withColumn("_avg_gain", F.avg("_gain").over(pw(period)))
        df = df.withColumn("_avg_loss", F.avg("_loss").over(pw(period)))
        df = df.withColumn(f"rsi_{period}",
            F.when(F.col("_avg_loss") == 0, F.lit(100.0))
             .otherwise(100.0 - 100.0 / (1.0 + F.col("_avg_gain") / F.col("_avg_loss")))
             .cast(DoubleType())
        )
        df = df.drop("_delta", "_gain", "_loss", "_avg_gain", "_avg_loss")

    # Stochastic Oscillator %K and %D (14-day)
    df = df.withColumn("_14d_high", F.max("high").over(pw(14)))
    df = df.withColumn("_14d_low",  F.min("low").over(pw(14)))
    df = df.withColumn("stoch_k",
        ((F.col("close") - F.col("_14d_low")) / (F.col("_14d_high") - F.col("_14d_low")) * 100)
        .cast(DoubleType())
    )
    df = df.withColumn("stoch_d", F.avg("stoch_k").over(pw(3)).cast(DoubleType()))  # 3-day SMA of %K
    df = df.drop("_14d_high", "_14d_low")

    # Williams %R (14-day)
    df = df.withColumn("_wr_high", F.max("high").over(pw(14)))
    df = df.withColumn("_wr_low",  F.min("low").over(pw(14)))
    df = df.withColumn("williams_r",
        (((F.col("_wr_high") - F.col("close")) / (F.col("_wr_high") - F.col("_wr_low"))) * -100)
        .cast(DoubleType())
    ).drop("_wr_high", "_wr_low")

    # CCI — Commodity Channel Index (14, 20)
    for period in [14, 20]:
        df = df.withColumn("_tp", ((F.col("high") + F.col("low") + F.col("close")) / 3))
        df = df.withColumn("_tp_sma", F.avg("_tp").over(pw(period)))
        df = df.withColumn("_mad",   # mean absolute deviation
            F.avg(F.abs(F.col("_tp") - F.col("_tp_sma"))).over(pw(period))
        )
        df = df.withColumn(f"cci_{period}",
            ((F.col("_tp") - F.col("_tp_sma")) / (0.015 * F.col("_mad"))).cast(DoubleType())
        ).drop("_tp", "_tp_sma", "_mad")

    # Rate of Change (ROC) — % change vs N days ago
    for period in [5, 10, 20]:
        df = df.withColumn(f"roc_{period}",
            ((F.col("close") / F.lag("close", period).over(rw())) - 1).cast(DoubleType())
        )

    # Momentum (raw price diff vs N days ago)
    df = df.withColumn("momentum_10",
        (F.col("close") - F.lag("close", 10).over(rw())).cast(DoubleType())
    )

    # Aroon (25-day): measures days since highest high / lowest low
    df = df.withColumn("_25d_high_idx", F.max("high").over(pw(25)))  # simplified
    df = df.withColumn("aroon_up",
        ((25 - (F.lit(25) - F.rank().over(
            Window.partitionBy("ticker").orderBy("date").rowsBetween(-25, 0)
        ))) / 25 * 100).cast(DoubleType())
    )
    df = df.withColumn("aroon_oscillator",
        (F.col("aroon_up") - (100 - F.col("aroon_up"))).cast(DoubleType())
    ).drop("_25d_high_idx")

    return df


# ── Volatility indicators ──────────────────────────────────────────────────────

def add_volatility(df: DataFrame) -> DataFrame:
    # Bollinger Bands (20, 2σ)
    df = df.withColumn("bb_mid",    F.avg("close").over(pw(20)).cast(DoubleType()))
    df = df.withColumn("_bb_std",   F.stddev("close").over(pw(20)))
    df = df.withColumn("bb_upper",  (F.col("bb_mid") + 2 * F.col("_bb_std")).cast(DoubleType()))
    df = df.withColumn("bb_lower",  (F.col("bb_mid") - 2 * F.col("_bb_std")).cast(DoubleType()))
    df = df.withColumn("bb_bandwidth", 
        ((F.col("bb_upper") - F.col("bb_lower")) / F.col("bb_mid")).cast(DoubleType())
    )
    df = df.drop("_bb_std")
    return df


# ── Execution Pipeline ─────────────────────────────────────────────────────────

def main() -> None:
    spark = get_spark()

    print("Reading silver.ohlcv...")
    # NOTE: Table is explicitly aliased to "s" to resolve [AMBIGUOUS_REFERENCE] issues
    silver = spark.read.table(SILVER_OHLCV).alias("s")

    print("Aggregating daily OHLCV...")
    # FIX: Scoped column references 's.ticker' and 's.date' eliminate ambiguity completely
    daily_df = silver.groupBy(F.col("s.ticker"), F.col("s.date")).agg(
        F.first("open").alias("open"),
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        F.last("close").alias("close"),
        F.sum("volume").alias("volume")
    )

    # Generate daily_return_pct required by downstream momentum calculations
    print("Calculating daily baseline returns...")
    daily_window = Window.partitionBy("ticker").orderBy("date")
    daily_df = daily_df.withColumn("_prev_close", F.lag("close", 1).over(daily_window))
    daily_df = daily_df.withColumn(
        "daily_return_pct",
        F.when(F.col("_prev_close").isNull() | (F.col("_prev_close") == 0), 0.0)
         .otherwise(((F.col("close") - F.col("_prev_close")) / F.col("_prev_close")) * 100)
         .cast(DoubleType())
    ).drop("_prev_close")

    # Add features sequentially
    print("Computing Trend features...")
    daily_df = add_trend(daily_df)

    print("Computing Momentum features...")
    daily_df = add_momentum(daily_df)

    print("Computing Volatility features...")
    daily_df = add_volatility(daily_df)

    # Save tracking data to Gold zone Iceberg Table
    print(f"Persisting results into Gold Layer...")
    write_iceberg(spark, daily_df, GOLD_OHLCV_DAILY)
    print("Pipeline execution complete.")


if __name__ == "__main__":
    main()