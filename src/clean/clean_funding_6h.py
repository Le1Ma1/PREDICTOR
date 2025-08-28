import io, sys
import polars as pl
import requests
from .supabase_utils import upsert_batch

TABLE = "clean_funding_6h"

def _read_ndjson(path_or_url: str) -> pl.DataFrame:
    if path_or_url.startswith(("http://","https://")):
        r = requests.get(path_or_url, timeout=60)
        r.raise_for_status()
        return pl.read_ndjson(io.BytesIO(r.content))
    return pl.read_ndjson(path_or_url)

def clean(path_or_url: str) -> pl.DataFrame:
    df = _read_ndjson(path_or_url)

    # ts_utc：優先用原始 _ts_utc；沒有就由 time(ms) 轉
    ts_expr = (
        pl.col("_ts_utc").alias("ts_utc")
        if "_ts_utc" in df.columns
        else pl.from_epoch(pl.col("time"), unit="ms").dt.strftime("%Y-%m-%dT%H:%M:%S+00:00").alias("ts_utc")
    )

    # 若缺欄則補預設；若存在就轉型
    sym_expr = pl.lit("BTCUSDT").alias("symbol") if "symbol" not in df.columns else pl.col("symbol").cast(pl.Utf8)
    ex_expr  = pl.lit("agg").alias("exchange")    if "exchange" not in df.columns else pl.col("exchange").cast(pl.Utf8)

    out = df.with_columns(
        ts_expr, sym_expr, ex_expr,
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
    ).select(["ts_utc","symbol","exchange","open","high","low","close"])

    # 去重（同鍵只留最後）
    out = out.unique(subset=["ts_utc","symbol","exchange"], keep="last")
    return out

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.clean.clean_funding_6h <path_or_url>")
        sys.exit(2)
    df = clean(sys.argv[1])
    upsert_batch(TABLE, df.to_dicts())
