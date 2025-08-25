import os, json
import pandas as pd
from supabase import create_client

# === Supabase 初始化 ===
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
sb = create_client(url, key)

BUCKET = os.getenv("SUPABASE_BUCKET", "lake")

def load_from_storage(path: str):
    """從 Supabase Storage 下載 JSONL 並轉 DataFrame"""
    res = sb.storage.from_(BUCKET).download(path)
    rows = [json.loads(line) for line in res.decode("utf-8").splitlines()]
    df = pd.DataFrame(rows)
    return df

def clean_liq_agg(df: pd.DataFrame):
    """清洗 liquidation aggregated"""
    df = df.rename(columns={
        "aggregated_long_liquidation_usd": "long_liq_usd",
        "aggregated_short_liquidation_usd": "short_liq_usd"
    })
    df = df[["_ts_utc", "long_liq_usd", "short_liq_usd"]]
    df["long_liq_usd"] = df["long_liq_usd"].astype(float)
    df["short_liq_usd"] = df["short_liq_usd"].astype(float)
    return df

def insert_to_db(df: pd.DataFrame, table="liq_agg_6h"):
    """批次寫入 Supabase DB"""
    rows = df.to_dict(orient="records")
    sb.table(table).upsert(rows).execute()

if __name__ == "__main__":
    # 這裡改成你最新的檔案路徑
    path = "liq_agg_6h/dt=2025-08-25/part-20250825103237_00603fc1.jsonl"
    raw = load_from_storage(path)
    df = clean_liq_agg(raw)
    insert_to_db(df)
    print(f"Inserted {len(df)} rows into liq_agg_6h")
