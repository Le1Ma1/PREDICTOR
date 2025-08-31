import os, json, requests, hashlib
from src.utils.env import supabase, COINGLASS_API_KEY, SUPABASE_BUCKET
from src.utils.time import to_utc_iso, utc_today, utc_ts

BASE = "https://open-api-v4.coinglass.com/api"
HEAD = {"accept":"application/json","CG-API-KEY":COINGLASS_API_KEY}
SRC_KEY = "funding_6h"
SYMBOL = "ETHUSDT"

def fetch():
    url = f"{BASE}/futures/funding-rate/history"
    params = {"exchange":"Binance","symbol":SYMBOL,"interval":"6h","limit":4500}
    r = requests.get(url, headers=HEAD, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()["data"]
    for d in data:
        d["_ts_utc"] = to_utc_iso(d["time"])
    return data

def upload(data):
    path = f"ETH/{SRC_KEY}/dt={utc_today()}/part-{utc_ts()}_{hashlib.md5(str(data[0]).encode()).hexdigest()[:8]}.jsonl"
    body = "\n".join(json.dumps(d) for d in data)
    supabase.storage.from_(SUPABASE_BUCKET).upload(path, body.encode(), {"content-type":"application/jsonl"})
    print(f"{SRC_KEY} done, {len(data)} rows → {path}")

if __name__=="__main__":
    upload(fetch())
