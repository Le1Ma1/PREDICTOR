import io, sys
import polars as pl
import requests
from ..supabase_utils import upsert_batch

TABLE = "clean_liq_agg_6h"

def _read_ndjson(path_or_url: str) -> pl.DataFrame:
    if path_or_url.startswith(("http://","https://")):
        r = requests.get(path_or_url, timeout=60)
        r.raise_for_status()
        return pl.read_ndjson(io.BytesIO(r.content))
    return pl.read_ndjson(path_or_url)

def clean(path_or_url: str) -> pl.DataFrame:
    df = _read_ndjson(path_or_url)

    # ts_utc：優先用 _ts_utc，否則從 time(ms)
    ts_expr = (
        pl.col("_ts_utc").alias("ts_utc")
        if "_ts_utc" in df.columns
        else pl.from_epoch(pl.col("time"), unit="ms")
             .dt.strftime("%Y-%m-%dT%H:%M:%S+00:00").alias("ts_utc")
    )

    # 來源欄位名稱為 aggregated_*，轉成統一命名
    long_expr  = pl.col("aggregated_long_liquidation_usd").cast(pl.Float64).alias("long_liq_usd")
    short_expr = pl.col("aggregated_short_liquidation_usd").cast(pl.Float64).alias("short_liq_usd")

    out = df.with_columns(
        ts_expr,
        pl.lit("ETH").alias("symbol"),
        long_expr,
        short_expr,
        (pl.col("aggregated_long_liquidation_usd").cast(pl.Float64)
         + pl.col("aggregated_short_liquidation_usd").cast(pl.Float64)).alias("total_liq_usd"),
    ).select(["ts_utc","symbol","long_liq_usd","short_liq_usd","total_liq_usd"])

    # 去重
    out = out.unique(subset=["ts_utc","symbol"], keep="last")
    return out

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.clean.clean_liq_agg_6h <path_or_url>")
        sys.exit(2)
    df = clean(sys.argv[1])
    upsert_batch(TABLE, df.to_dicts())
