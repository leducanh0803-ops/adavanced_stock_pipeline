
from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
import os

SILVER_NAMESPACE = "nessie.silver"
GOLD_NAMESPACE   = "nessie.gold"
SILVER_OHLCV     = f"{SILVER_NAMESPACE}.ohlcv"
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
    print(f"  ✓ {table}: {count:,} rows")


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
    df = df.withColumn("bb_mid",   F.avg("close").over(pw(20)).cast(DoubleType()))
    df = df.withColumn("_bb_std",  F.stddev("close").over(pw(20)))
    df = df.withColumn("bb_upper", (F.col("bb_mid") + 2*F.col("_bb_std")).cast(DoubleType()))
    df = df.withColumn("bb_lower", (F.col("bb_mid") - 2*F.col("_bb_std")).cast(DoubleType()))
    df = df.withColumn("bb_width",
        ((F.col("bb_upper") - F.col("bb_lower")) / F.col("bb_mid")).cast(DoubleType())
    )
    df = df.withColumn("bb_pct",
        ((F.col("close") - F.col("bb_lower")) / (F.col("bb_upper") - F.col("bb_lower"))).cast(DoubleType())
    ).drop("_bb_std")

    # ATR (7, 14, 21)
    df = df.withColumn("_prev_close", F.lag("close", 1).over(rw()))
    df = df.withColumn("_tr",
        F.greatest(
            F.col("high") - F.col("low"),
            F.abs(F.col("high") - F.col("_prev_close")),
            F.abs(F.col("low")  - F.col("_prev_close"))
        )
    )
    for period in [7, 14, 21]:
        df = df.withColumn(f"atr_{period}", F.avg("_tr").over(pw(period)).cast(DoubleType()))

    # Keltner Channels (based on ATR14, EMA20)
    df = df.withColumn("keltner_upper", (F.col("ema_21") + 2*F.col("atr_14")).cast(DoubleType()))
    df = df.withColumn("keltner_lower", (F.col("ema_21") - 2*F.col("atr_14")).cast(DoubleType()))

    df = df.drop("_prev_close", "_tr")

    # Historical Volatility (annualised std of daily returns)
    for period in [10, 20, 30]:
        df = df.withColumn(f"hist_vol_{period}",
            (F.stddev("daily_return_pct").over(pw(period)) * F.sqrt(F.lit(252))).cast(DoubleType())
        )

    # Ulcer Index (14-day) — measures downside risk
    df = df.withColumn("_14d_max", F.max("close").over(pw(14)))
    df = df.withColumn("_drawdown_pct",
        ((F.col("close") - F.col("_14d_max")) / F.col("_14d_max") * 100)
    )
    df = df.withColumn("ulcer_index",
        F.sqrt(F.avg(F.col("_drawdown_pct")**2).over(pw(14))).cast(DoubleType())
    ).drop("_14d_max", "_drawdown_pct")

    # Average True Range %  (ATR relative to price — normalised)
    df = df.withColumn("atr_pct", (F.col("atr_14") / F.col("close") * 100).cast(DoubleType()))

    return df


# ── Volume indicators ──────────────────────────────────────────────────────────

def add_volume(df: DataFrame) -> DataFrame:
    # VWAP is already in ohlcv_daily; recompute rolling VWAP (5/20 day)
    for period in [5, 20]:
        df = df.withColumn(f"vwap_{period}d",
            (F.sum(F.col("close") * F.col("volume")).over(pw(period)) /
             F.sum("volume").over(pw(period))).cast(DoubleType())
        )

    # OBV — On Balance Volume (cumulative)
    df = df.withColumn("_prev_close_obv", F.lag("close", 1).over(rw()))
    df = df.withColumn("_obv_delta",
        F.when(F.col("close") > F.col("_prev_close_obv"),  F.col("volume"))
         .when(F.col("close") < F.col("_prev_close_obv"), -F.col("volume"))
         .otherwise(F.lit(0))
    )
    df = df.withColumn("obv", F.sum("_obv_delta").over(ew()).cast(DoubleType()))
    df = df.withColumn("obv_sma_20", F.avg("obv").over(pw(20)).cast(DoubleType()))
    df = df.drop("_prev_close_obv", "_obv_delta")

    # CMF — Chaikin Money Flow (20-day)
    df = df.withColumn("_mf_mult",
        ((F.col("close") - F.col("low")) - (F.col("high") - F.col("close"))) /
        (F.col("high") - F.col("low"))
    )
    df = df.withColumn("_mf_vol", F.col("_mf_mult") * F.col("volume"))
    df = df.withColumn("cmf_20",
        (F.sum("_mf_vol").over(pw(20)) / F.sum("volume").over(pw(20))).cast(DoubleType())
    ).drop("_mf_mult", "_mf_vol")

    # MFI — Money Flow Index (14-day)
    df = df.withColumn("_tp", (F.col("high") + F.col("low") + F.col("close")) / 3)
    df = df.withColumn("_raw_mf", F.col("_tp") * F.col("volume"))
    df = df.withColumn("_prev_tp", F.lag("_tp", 1).over(rw()))
    df = df.withColumn("_pos_mf", F.when(F.col("_tp") > F.col("_prev_tp"), F.col("_raw_mf")).otherwise(0.0))
    df = df.withColumn("_neg_mf", F.when(F.col("_tp") < F.col("_prev_tp"), F.col("_raw_mf")).otherwise(0.0))
    df = df.withColumn("_pos_sum", F.sum("_pos_mf").over(pw(14)))
    df = df.withColumn("_neg_sum", F.sum("_neg_mf").over(pw(14)))
    df = df.withColumn("mfi_14",
        F.when(F.col("_neg_sum") == 0, F.lit(100.0))
         .otherwise(100.0 - 100.0 / (1.0 + F.col("_pos_sum") / F.col("_neg_sum")))
         .cast(DoubleType())
    ).drop("_tp", "_raw_mf", "_prev_tp", "_pos_mf", "_neg_mf", "_pos_sum", "_neg_sum")

    # Volume ratios and metrics
    df = df.withColumn("volume_sma_20", F.avg("volume").over(pw(20)).cast(DoubleType()))
    df = df.withColumn("volume_ratio",
        (F.col("volume") / F.col("volume_sma_20")).cast(DoubleType())
    )
    df = df.withColumn("dollar_volume",
        (F.col("close") * F.col("volume")).cast(DoubleType())
    )

    # Force Index: close change * volume
    df = df.withColumn("force_index_13",
        F.avg(
            (F.col("close") - F.lag("close", 1).over(rw())) * F.col("volume")
        ).over(pw(13)).cast(DoubleType())
    )

    # Ease of Movement (EMV 14-day)
    df = df.withColumn("_prev_hl_mid",
        (F.lag("high", 1).over(rw()) + F.lag("low", 1).over(rw())) / 2
    )
    df = df.withColumn("_hl_mid", (F.col("high") + F.col("low")) / 2)
    df = df.withColumn("_emv_raw",
        ((F.col("_hl_mid") - F.col("_prev_hl_mid")) /
         (F.col("volume") / (F.col("high") - F.col("low") + F.lit(1e-9))))
    )
    df = df.withColumn("emv_14",
        F.avg("_emv_raw").over(pw(14)).cast(DoubleType())
    ).drop("_prev_hl_mid", "_hl_mid", "_emv_raw")

    # Volume oscillator: (fast vol SMA - slow vol SMA) / slow vol SMA
    df = df.withColumn("volume_oscillator",
        ((F.avg("volume").over(pw(5)) - F.avg("volume").over(pw(20))) /
         F.avg("volume").over(pw(20))).cast(DoubleType())
    )

    return df


# ── Candlestick features ───────────────────────────────────────────────────────

def add_candle_features(df: DataFrame) -> DataFrame:
    df = df.withColumn("candle_body",
        F.abs(F.col("close") - F.col("open")).cast(DoubleType())
    )
    df = df.withColumn("candle_range",
        (F.col("high") - F.col("low")).cast(DoubleType())
    )
    df = df.withColumn("candle_body_pct",
        (F.col("candle_body") / (F.col("candle_range") + F.lit(1e-9))).cast(DoubleType())
    )
    df = df.withColumn("upper_shadow",
        (F.col("high") - F.greatest("open", "close")).cast(DoubleType())
    )
    df = df.withColumn("lower_shadow",
        (F.least("open", "close") - F.col("low")).cast(DoubleType())
    )
    df = df.withColumn("upper_shadow_pct",
        (F.col("upper_shadow") / (F.col("candle_range") + F.lit(1e-9))).cast(DoubleType())
    )
    df = df.withColumn("lower_shadow_pct",
        (F.col("lower_shadow") / (F.col("candle_range") + F.lit(1e-9))).cast(DoubleType())
    )
    df = df.withColumn("is_bullish",
        (F.col("close") > F.col("open")).cast(IntegerType())
    )
    df = df.withColumn("is_doji",
        (F.col("candle_body_pct") < 0.1).cast(IntegerType())
    )
    df = df.withColumn("gap_pct",
        ((F.col("open") / F.lag("close", 1).over(rw())) - 1).cast(DoubleType())
    )
    df = df.withColumn("is_gap_up",
        (F.col("gap_pct") > 0.01).cast(IntegerType())
    )
    df = df.withColumn("is_gap_down",
        (F.col("gap_pct") < -0.01).cast(IntegerType())
    )
    # Inside bar: today's high < prev high AND low > prev low
    df = df.withColumn("is_inside_bar",
        ((F.col("high") < F.lag("high", 1).over(rw())) &
         (F.col("low")  > F.lag("low",  1).over(rw()))).cast(IntegerType())
    )
    return df


# ── ML-specific features ───────────────────────────────────────────────────────

def add_ml_features(df: DataFrame) -> DataFrame:
    # ─ Lag features ─────────────────────────────────────────────────────
    for lag in [1, 2, 3, 4, 5, 10, 20]:
        df = df.withColumn(f"close_lag_{lag}",
            F.lag("close", lag).over(rw()).cast(DoubleType())
        )
        df = df.withColumn(f"return_lag_{lag}",
            F.lag("daily_return_pct", lag).over(rw()).cast(DoubleType())
        )

    # ─ Rolling stats (ML loves these) ───────────────────────────────────
    for period in [5, 10, 20]:
        df = df.withColumn(f"return_mean_{period}d",
            F.avg("daily_return_pct").over(pw(period)).cast(DoubleType())
        )
        df = df.withColumn(f"return_std_{period}d",
            F.stddev("daily_return_pct").over(pw(period)).cast(DoubleType())
        )
        df = df.withColumn(f"return_min_{period}d",
            F.min("daily_return_pct").over(pw(period)).cast(DoubleType())
        )
        df = df.withColumn(f"return_max_{period}d",
            F.max("daily_return_pct").over(pw(period)).cast(DoubleType())
        )
        df = df.withColumn(f"high_max_{period}d",
            F.max("high").over(pw(period)).cast(DoubleType())
        )
        df = df.withColumn(f"low_min_{period}d",
            F.min("low").over(pw(period)).cast(DoubleType())
        )

    # ─ Z-score normalisation (close vs rolling mean/std) ────────────────
    for period in [20, 50, 200]:
        df = df.withColumn(f"close_zscore_{period}",
            ((F.col("close") - F.avg("close").over(pw(period))) /
             (F.stddev("close").over(pw(period)) + F.lit(1e-9))).cast(DoubleType())
        )

    # ─ Forward return labels (TARGET VARIABLES for supervised ML) ────────
    for horizon in [1, 5, 10, 20]:
        df = df.withColumn(f"fwd_return_{horizon}d",
            ((F.lead("close", horizon).over(rw()) / F.col("close")) - 1).cast(DoubleType())
        )
        df = df.withColumn(f"fwd_label_{horizon}d",
            (F.col(f"fwd_return_{horizon}d") > 0).cast(IntegerType())
        )

    # ─ Calendar features ─────────────────────────────────────────────────
    df = df.withColumn("day_of_week",   F.dayofweek("date").cast(IntegerType()))
    df = df.withColumn("day_of_month",  F.dayofmonth("date").cast(IntegerType()))
    df = df.withColumn("week_of_year",  F.weekofyear("date").cast(IntegerType()))
    df = df.withColumn("month",         F.month("date").cast(IntegerType()))
    df = df.withColumn("quarter",       F.quarter("date").cast(IntegerType()))
    df = df.withColumn("is_month_start",
        (F.dayofmonth("date") <= 5).cast(IntegerType())
    )
    df = df.withColumn("is_month_end",
        (F.dayofmonth(F.last_day("date")) == F.dayofmonth("date")).cast(IntegerType())
    )

    # ─ Cross-sectional percentile ranks ──────────────────────────────────
    cs_dense = Window.partitionBy("date").orderBy("close")
    for col, w in [
        ("close",          Window.partitionBy("date").orderBy("close")),
        ("daily_return_pct", Window.partitionBy("date").orderBy("daily_return_pct")),
        ("rsi_14",         Window.partitionBy("date").orderBy("rsi_14")),
        ("volume_ratio",   Window.partitionBy("date").orderBy("volume_ratio")),
        ("dollar_volume",  Window.partitionBy("date").orderBy("dollar_volume")),
    ]:
        df = df.withColumn(f"cs_rank_{col}",
            (F.percent_rank().over(w) * 100).cast(DoubleType())
        )

    # ─ Regime flags ──────────────────────────────────────────────────────
    df = df.withColumn("vol_regime",
        F.when(F.col("hist_vol_20") < 20, F.lit("low"))
         .when(F.col("hist_vol_20") < 40, F.lit("mid"))
         .otherwise(F.lit("high"))
    )
    df = df.withColumn("trend_regime",
        F.when(
            (F.col("close") > F.col("sma_50")) & (F.col("sma_50") > F.col("sma_200")),
            F.lit("uptrend")
        ).when(
            (F.col("close") < F.col("sma_50")) & (F.col("sma_50") < F.col("sma_200")),
            F.lit("downtrend")
        ).otherwise(F.lit("sideways"))
    )
    df = df.withColumn("rsi_regime",
        F.when(F.col("rsi_14") < 30, F.lit("oversold"))
         .when(F.col("rsi_14") > 70, F.lit("overbought"))
         .otherwise(F.lit("neutral"))
    )

    return df


# ── Composite scoring ──────────────────────────────────────────────────────────

def add_scoring(df: DataFrame) -> DataFrame:
    df = df.withColumn("trend_score",
        (
            F.when(F.col("close")  > F.col("sma_20"),  F.lit(20)).otherwise(0) +
            F.when(F.col("sma_20") > F.col("sma_50"),  F.lit(20)).otherwise(0) +
            F.when(F.col("sma_50") > F.col("sma_200"), F.lit(20)).otherwise(0) +
            F.when(F.col("macd")   > 0,                F.lit(20)).otherwise(0) +
            F.when(F.col("close")  > F.col("ichimoku_cloud_top"), F.lit(20)).otherwise(0)
        ).cast(DoubleType())
    )
    df = df.withColumn("momentum_score",
        (
            F.when((F.col("rsi_14") >= 40) & (F.col("rsi_14") <= 70), F.lit(25)).otherwise(0) +
            F.when(F.col("macd_hist") > 0,         F.lit(20)).otherwise(0) +
            F.when(F.col("daily_return_pct") > 0,  F.lit(15)).otherwise(0) +
            F.when(F.col("stoch_k") > F.col("stoch_d"), F.lit(20)).otherwise(0) +
            F.when(F.col("roc_10") > 0,            F.lit(20)).otherwise(0)
        ).cast(DoubleType())
    )
    df = df.withColumn("volatility_score",
        F.when(F.col("hist_vol_20") < 15, F.lit(100.0))
         .when(F.col("hist_vol_20") < 25, F.lit(80.0))
         .when(F.col("hist_vol_20") < 40, F.lit(60.0))
         .when(F.col("hist_vol_20") < 60, F.lit(40.0))
         .when(F.col("hist_vol_20") < 80, F.lit(20.0))
         .otherwise(F.lit(0.0)).cast(DoubleType())
    )
    df = df.withColumn("volume_score",
        (
            F.when(F.col("volume_ratio") >= 2.0, F.lit(40)).otherwise(0) +
            F.when(F.col("cmf_20") > 0,          F.lit(30)).otherwise(0) +
            F.when(F.col("mfi_14") > 50,         F.lit(30)).otherwise(0)
        ).cast(DoubleType())
    )
    df = df.withColumn("composite_score",
        (
            F.col("trend_score")      * 0.35 +
            F.col("momentum_score")   * 0.30 +
            F.col("volatility_score") * 0.20 +
            F.col("volume_score")     * 0.15
        ).cast(DoubleType())
    )
    return df


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")

    print("Reading silver.ohlcv...")
    silver = spark.table(SILVER_OHLCV)
    silver = (
        silver
        .withColumn("ts",   F.from_unixtime(F.col("datetime") / NANOS_PER_SEC))
        .withColumn("date", F.to_date("ts"))
        .withColumnRenamed("source", "ticker")
        .withColumnRenamed("Open",   "open")
        .withColumnRenamed("High",   "high")
        .withColumnRenamed("Low",    "low")
        .withColumnRenamed("Close",  "close")
        .withColumnRenamed("Volume", "volume")
    )

    # ── 1. Daily OHLCV aggregation ──────────────────────────────────────────
    print("Aggregating daily OHLCV...")
    daily = (
        silver.groupBy("ticker", "date")
        .agg(
            F.first("open").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.last("close").alias("close"),
            F.sum("volume").alias("volume"),
            (F.sum(F.col("close") * F.col("volume")) /
             F.sum("volume")).alias("vwap"),
        )
    )
    print("Writing gold.ohlcv_daily...")
    write_iceberg(spark, daily, f"{GOLD_NAMESPACE}.ohlcv_daily")

    # ── 2. Daily return % ───────────────────────────────────────────────────
    print("Computing indicators...")
    ind = daily
    ind = ind.withColumn("daily_return_pct",
        ((F.col("close") / F.lag("close", 1).over(rw())) - 1).cast(DoubleType())
    )

    # ── 3. All indicators ───────────────────────────────────────────────────
    ind = add_trend(ind)
    ind = add_momentum(ind)
    ind = add_volatility(ind)
    ind = add_volume(ind)
    ind = add_candle_features(ind)
    ind = add_ml_features(ind)
    ind = add_scoring(ind)

    print("Writing gold.indicators_daily...")
    write_iceberg(spark, ind, f"{GOLD_NAMESPACE}.indicators_daily")

    # ── 4. Daily rankings ───────────────────────────────────────────────────
    print("Computing rankings...")
    rank_w = Window.partitionBy("date").orderBy(F.desc("composite_score"))
    rankings = ind.select(
        "date", "ticker", "close", "daily_return_pct",
        "rsi_14", "macd", "macd_hist", "macd_crossover",
        "stoch_k", "stoch_d", "williams_r",
        "bb_pct", "atr_14", "hist_vol_20",
        "volume_ratio", "cmf_20", "mfi_14", "obv",
        "trend_regime", "vol_regime", "rsi_regime",
        "trend_score", "momentum_score", "volatility_score",
        "volume_score", "composite_score",
        "fwd_return_1d", "fwd_return_5d", "fwd_return_10d", "fwd_return_20d",
        "fwd_label_1d", "fwd_label_5d", "fwd_label_10d", "fwd_label_20d",
        "cs_rank_close", "cs_rank_daily_return_pct", "cs_rank_rsi_14",
        "cs_rank_volume_ratio", "cs_rank_dollar_volume",
    ).withColumn("rank", F.rank().over(rank_w))

    print("Writing gold.rankings_daily...")
    write_iceberg(spark, rankings, f"{GOLD_NAMESPACE}.rankings_daily")

    print("\nGold tables:")
    spark.sql(f"SHOW TABLES IN {GOLD_NAMESPACE}").show()
    spark.stop()


if __name__ == "__main__":
    main()