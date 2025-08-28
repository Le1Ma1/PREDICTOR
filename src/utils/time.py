import datetime as dt

def to_utc_iso(ms: int) -> str:
    """毫秒時間戳 → ISO UTC 時間字串"""
    return dt.datetime.fromtimestamp(int(ms)/1000, dt.timezone.utc).isoformat()

def utc_today() -> str:
    """今天日期 (YYYY-MM-DD, UTC)"""
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

def utc_ts() -> str:
    """現在時間 (YYYYMMDDHHMMSS, UTC)"""
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")
