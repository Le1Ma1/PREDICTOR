import os, io, json, hashlib, datetime as dt, requests
from dotenv import load_dotenv; load_dotenv()
from supabase import create_client

# === 基本設定 ===
BASE = "https://open-api-v4.coinglass.com/api"
HEAD = {"accept": "application/json", "CG-API-KEY": os.getenv("CG_API_KEY")}

SB = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
BUK = os.getenv("SUPABASE_BUCKET", "lake")

EX = os.getenv("EXCHANGE", "binance")
PAIR = os.getenv("SYMBOL_PAIR", "BTCUSDT")
ASSET = os.getenv("SYMBOL_ASSET", "BTC")

def now(): return dt.datetime.now(dt.timezone.utc)
def ms(t): return int(t.timestamp()*1000)

# === Supabase helpers ===
def up(path, content: bytes):
    SB.storage.from_(BUK).upload(path, content, {"content-type":"application/json","upsert":"true"})

def get_state(k):
    r = SB.table("ingest_state").select("*").eq("src_key", k).execute().data
    return r[0]["last_ts_utc"] if r else None

def set_state(k, iso):
    SB.table("ingest_state").upsert({"src_key": k, "last_ts_utc": iso}).execute()

def log_run(k, p, c, tmin, tmax, path):
    SB.table("ingest_log").insert({
        "src_key": k, "params": p, "count": c,
        "t_min": tmin, "t_max": tmax, "storage_path": path
    }).execute()
    SB.table("ingest_heartbeat").insert({"src_key": k}).execute()

# === API 呼叫 ===
def call(ep, params):
    r = requests.get(f"{BASE}/{ep}", headers=HEAD, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    return j.get("data", j)

# === 收數主程式 ===
def run_one(src_key, ep, pfmt, full_history=False):
    all_rows = []
    tmin = tmax = None
    start = None

    while True:
        params = pfmt(start, None)
        data = call(ep, params)
        if not data: break

        batch_min = batch_max = None
        rows = []
        for row in data:
            ts_ms = row.get("time") or row.get("t") or row.get("timestamp")
            if ts_ms:
                ts_val = int(ts_ms)
                ts = dt.datetime.fromtimestamp(ts_val/1000 if ts_val > 1e12 else ts_val, dt.timezone.utc)
                row["_ts_utc"] = ts.isoformat()
                batch_min = ts if not batch_min else min(batch_min, ts)
                batch_max = ts if not batch_max else max(batch_max, ts)
            rows.append(row)

        if not rows: break
        all_rows.extend(rows)

        # 更新範圍
        tmin = batch_min if not tmin else min(tmin, batch_min)
        tmax = batch_max if not tmax else max(tmax, batch_max)

        # 下一輪從最新時間 + 1 秒開始
        start = int(batch_max.timestamp() * 1000) + 1000

        # 如果不是 full_history，就只跑一次
        if not full_history: break

        # 若回來不足一批，代表到底了
        if len(rows) < 4500: break

    # === 存檔 ===
    if not all_rows: return
    today = now().strftime("%Y-%m-%d")
    ph = hashlib.md5(json.dumps(params, sort_keys=True).encode()).hexdigest()[:8]
    path = f"{src_key}/dt={today}/part-{now().strftime('%Y%m%d%H%M%S')}_{ph}.jsonl"

    buf = io.StringIO()
    for r in all_rows:
        buf.write(json.dumps(r, ensure_ascii=False)+"\n")

    up(path, buf.getvalue().encode("utf-8"))
    log_run(src_key, params, len(all_rows),
            tmin.isoformat() if tmin else None,
            tmax.isoformat() if tmax else None,
            path)
    if tmax: set_state(src_key, tmax.isoformat())
    print(f"{src_key} done, {len(all_rows)} rows, {tmin}~{tmax}")

# === 兩個端點 ===
if __name__ == "__main__":
    run_one(
        "liq_agg_6h",
        "futures/liquidation/aggregated-history",
        lambda f,t: {
            "exchange_list": EX,
            "symbol": ASSET,
            "interval": "6h",
            **({"start_time": f} if f else {}),
            "limit": 4500
        },
        full_history=True
    )

    run_one(
        "taker_flow_6h",
        "futures/taker-buy-sell-volume/history",
        lambda f,t: {
            "exchange": EX,
            "symbol": PAIR,
            "interval": "6h",
            **({"from": f} if f else {}),
            "limit": 4500
        },
        full_history=True
    )
