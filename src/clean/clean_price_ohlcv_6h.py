# src/clean/clean_price_ohlcv_6h.py
import io, sys
import polars as pl
import requests
from .supabase_utils import upsert_batch

TABLE = "clean_price_ohlcv_6h"

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
    sym_expr = pl.col("symbol").cast(pl.Utf8) if "symbol" in df.columns else pl.lit("BTCUSDT").alias("symbol")

    out = df.with_columns(
        ts_expr,
        sym_expr,
        pl.col("open").cast(pl.Float64, strict=False),
        pl.col("high").cast(pl.Float64, strict=False),
        pl.col("low").cast(pl.Float64, strict=False),
        pl.col("close").cast(pl.Float64, strict=False),
        pl.col("volume_usd").cast(pl.Float64, strict=False),
    ).select(["ts_utc","symbol","open","high","low","close","volume_usd"])

    out = out.unique(subset=["ts_utc","symbol"], keep="last")
    return out

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.clean.clean_price_ohlcv_6h <path_or_url>")
        sys.exit(2)
    df = clean(sys.argv[1])
    upsert_batch(TABLE, df.to_dicts())
