import io, sys
import polars as pl
import requests
from .supabase_utils import upsert_batch

TABLE = "clean_lsr_6h"

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

    ratio_col = (
        "longShortRatio"
        if "longShortRatio" in df.columns
        else ("global_account_long_short_ratio" if "global_account_long_short_ratio" in df.columns else None)
    )
    if ratio_col is None:
        raise ValueError("缺 long-short ratio 欄位")

    out = df.with_columns(
        ts_expr,
        (pl.col("symbol").cast(pl.Utf8) if "symbol" in df.columns else pl.lit("BTC").alias("symbol")),
        (pl.col("exchange").cast(pl.Utf8) if "exchange" in df.columns else pl.lit("global").alias("exchange")),
        pl.col(ratio_col).cast(pl.Float64).alias("long_short_ratio"),
        (pl.col("long_volume_usd").cast(pl.Float64) if "long_volume_usd" in df.columns else pl.lit(None, dtype=pl.Float64)).alias("long_volume_usd"),
        (pl.col("short_volume_usd").cast(pl.Float64) if "short_volume_usd" in df.columns else pl.lit(None, dtype=pl.Float64)).alias("short_volume_usd"),
    ).select(["ts_utc","symbol","exchange","long_short_ratio","long_volume_usd","short_volume_usd"])

    out = out.unique(subset=["ts_utc","symbol","exchange"], keep="last")
    return out

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.clean.clean_lsr_6h <path_or_url>")
        sys.exit(2)
    df = clean(sys.argv[1])
    upsert_batch(TABLE, df.to_dicts())
