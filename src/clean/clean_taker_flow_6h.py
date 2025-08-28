import io, sys
import polars as pl
import requests
from .supabase_utils import upsert_batch

TABLE = "clean_taker_flow_6h"

def _read_ndjson(path_or_url: str) -> pl.DataFrame:
    if path_or_url.startswith(("http://","https://")):
        r = requests.get(path_or_url, timeout=60)
        r.raise_for_status()
        return pl.read_ndjson(io.BytesIO(r.content))
    return pl.read_ndjson(path_or_url)

def clean(path_or_url: str) -> pl.DataFrame:
    df = _read_ndjson(path_or_url)

    ts_expr = (
        pl.col("_ts_utc").alias("ts_utc")
        if "_ts_utc" in df.columns
        else pl.from_epoch(pl.col("time"), unit="ms").dt.strftime("%Y-%m-%dT%H:%M:%S+00:00").alias("ts_utc")
    )

    buy_name  = "taker_buy_volume_usd"  if "taker_buy_volume_usd"  in df.columns else "buy_volume_usd"
    sell_name = "taker_sell_volume_usd" if "taker_sell_volume_usd" in df.columns else "sell_volume_usd"

    out = df.with_columns(
        ts_expr,
        (pl.col("symbol").cast(pl.Utf8)   if "symbol"   in df.columns else pl.lit("BTCUSDT").alias("symbol")),
        (pl.col("exchange").cast(pl.Utf8) if "exchange" in df.columns else pl.lit("agg").alias("exchange")),
        pl.col(buy_name).cast(pl.Float64).alias("buy_volume_usd"),
        pl.col(sell_name).cast(pl.Float64).alias("sell_volume_usd"),
    ).with_columns(
        pl.when((pl.col("buy_volume_usd") + pl.col("sell_volume_usd")) > 0)
          .then(pl.col("buy_volume_usd") / (pl.col("buy_volume_usd") + pl.col("sell_volume_usd")))
          .otherwise(None)
          .alias("taker_ratio")
    ).select(["ts_utc","symbol","exchange","buy_volume_usd","sell_volume_usd","taker_ratio"])

    out = out.unique(subset=["ts_utc","symbol","exchange"], keep="last")
    return out

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.clean.clean_taker_flow_6h <path_or_url>")
        sys.exit(2)
    df = clean(sys.argv[1])
    upsert_batch(TABLE, df.to_dicts())
