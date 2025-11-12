
"""
PySpark Data Quality checks.
Spark 3.x compatible. Functions return a DataFrame of failures for easy debugging.
"""
from typing import Iterable, Any, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

def not_null(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    cond = None
    for c in columns:
        expr = F.col(c).isNull()
        cond = expr if cond is None else (cond | expr)
    return df.where(cond)

def unique(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    key = list(columns)
    dup_keys = df.groupBy(*key).count().where(F.col("count") > 1).drop("count")
    return df.join(dup_keys, on=key, how="inner")

def in_set(df: DataFrame, column: str, allowed: Iterable[Any]) -> DataFrame:
    return df.where(~F.col(column).isin(list(allowed)) & F.col(column).isNotNull())

def between(df: DataFrame, column: str, low: float, high: float, inclusive: str = "both") -> DataFrame:
    c = F.col(column)
    if inclusive == "both":
        bad = ~((c >= F.lit(low)) & (c <= F.lit(high)))
    elif inclusive == "left":
        bad = ~((c >= F.lit(low)) & (c < F.lit(high)))
    elif inclusive == "right":
        bad = ~((c > F.lit(low)) & (c <= F.lit(high)))
    else:
        bad = ~((c > F.lit(low)) & (c < F.lit(high)))
    return df.where(bad & c.isNotNull())

def non_negative(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    cond = None
    for c in columns:
        expr = F.col(c) < 0
        cond = expr if cond is None else (cond | expr)
    return df.where(cond)

def parseable_date(df: DataFrame, column: str, fmt: str) -> DataFrame:
    parsed = F.to_timestamp(F.col(column), fmt)
    return df.where(parsed.isNull() & F.col(column).isNotNull())

def time_order(df: DataFrame, start_col: str, end_col: str, time_fmt: str = "HH:mm:ss") -> DataFrame:
    s = F.to_timestamp(F.col(start_col), time_fmt)
    e = F.to_timestamp(F.col(end_col), time_fmt)
    return df.where(e < s)

def arithmetic_equal(df: DataFrame, target_col: str, expr_cols: Iterable[str], tol: float = 1e-6) -> DataFrame:
    s = None
    for c in expr_cols:
        s = F.col(c) if s is None else (s + F.col(c))
    return df.where(F.abs(F.col(target_col) - s) > tol)

def iqr_outliers(df: DataFrame, column: str, k: float = 1.5) -> DataFrame:
    # approximate quantiles are fine for DQ
    q1, q3 = df.approxQuantile(column, [0.25, 0.75], 0.01)
    low, high = q1 - k*(q3 - q1), q3 + k*(q3 - q1)
    return df.where((F.col(column) < low) | (F.col(column) > high))
