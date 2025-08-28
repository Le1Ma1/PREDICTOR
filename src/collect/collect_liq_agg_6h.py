import os, json, time, datetime as dt, hashlib, requests
from supabase import create_client
from src.utils.env import supabase, COINGLASS_API_KEY, SUPABASE_BUCKET

# === 環境變數 ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "lake")
COINGLASS_API_KEY = os.getenv("CG_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ SUPABASE_URL / SUPABASE_KEY 沒有正確載入，請檢查 .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE = "https://open-api-v4.coinglass.com/api"
HEADERS = {"accept": "application/json", "CG-API-KEY": COINGLASS_API_KEY}

SRC_KEY = "liq_agg_6h"

# === API 抓取 ===
def fetch_all(symbol="BTC", interval="6h", exchanges="binance,okx,bybit"):
    url = f"{BASE}/futures/liquidation/aggregated-history"
    now = int(time.time() * 1000)   # 當前時間（ms）
    start = 1609459200000           # 2021-01-01 00:00:00 UTC 起
    step = 1000 * 60 * 60 * 24 * 30 # 每次拉 30 天，避免超量

    all_data = []
    while start < now:
        end = min(start + step, now)
        params = {
            "exchange_list": exchanges,
            "symbol": symbol,
            "interval": interval,
            "limit": 4500,
            "start_time": start,
            "end_time": end
        }
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        js = r.json()

        if "data" not in js:
            print("⚠️ Error response:", js)
            break

        data = js["data"]
        for d in data:
            ts_ms = int(d["time"]) * (1000 if len(str(d["time"])) == 10 else 1)
            d["_ts_utc"] = dt.datetime.fromtimestamp(ts_ms/1000, dt.timezone.utc).isoformat()
        all_data.extend(data)

        print(f"✅ Fetched {len(data)} rows from {dt.datetime.utcfromtimestamp(start/1000)} "
              f"to {dt.datetime.utcfromtimestamp(end/1000)}")
        start = end
        time.sleep(0.2)  # 避免 API rate limit

    return all_data

# === 上傳 Supabase Storage ===
def upload(data):
    today = dt.datetime.utcnow().strftime("%Y-%m-%d")
    ts = dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    h = hashlib.md5(json.dumps(data).encode()).hexdigest()[:8]

    path = f"{SRC_KEY}/dt={today}/part-{ts}_{h}.jsonl"
    body = "\n".join(json.dumps(d) for d in data)

    supabase.storage.from_(SUPABASE_BUCKET).upload(path, body.encode("utf-8"))
    print(f"📤 Uploaded {len(data)} rows → {path}")

# === 主程式 ===
if __name__ == "__main__":
    data = fetch_all()
    if data:
        upload(data)
