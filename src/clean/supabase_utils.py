from typing import List, Dict

try:
    from src.utils.env import supabase  
except Exception:
    from supabase import create_client
    from ..env import SUPABASE_URL, SUPABASE_KEY  
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def upsert_batch(table: str, rows: List[Dict], batch_size: int = 500):
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i+batch_size]
        if not chunk:
            continue
        supabase.table(table).upsert(chunk).execute()
        print(f"✅ Upserted {len(chunk)} rows → {table}")