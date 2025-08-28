import json, datetime as dt, hashlib, requests
from src.utils.env import supabase, COINGLASS_API_KEY, SUPABASE_BUCKET

BASE = "https://open-api-v4.coinglass.com/api"
HEAD = {"accept":"application/json","CG-API-KEY":COINGLASS_API_KEY}
SRC_KEY = "agg_oi_6h"

def fetch():
    url = f"{BASE}/futures/open-interest/aggregated-history"
    params = {"symbol":"BTC","interval":"6h","limit":4500}
    r = requests.get(url, headers=HEAD, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()["data"]
    for d in data:
        d["_ts_utc"] = dt.datetime.fromtimestamp(int(d["time"])/1000, dt.timezone.utc).isoformat()
    return data

def upload(data):
    today = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")
    h = hashlib.md5(str(data[0]).encode()).hexdigest()[:8]
    path = f"{SRC_KEY}/dt={today}/part-{ts}_{h}.jsonl"
    body = "\n".join(json.dumps(d) for d in data)
    supabase.storage.from_(SUPABASE_BUCKET).upload(path, body.encode(), {"content-type":"application/jsonl"})
    print(f"{SRC_KEY} done, {len(data)} rows → {path}")

if __name__=="__main__":
    upload(fetch())
