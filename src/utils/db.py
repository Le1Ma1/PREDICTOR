import os
import pandas as pd
from sqlalchemy import create_engine

def get_engine(url_env="SUPABASE_DB_URL"):
    url = os.getenv(url_env)
    if not url:
        raise RuntimeError(f"{url_env} not set")
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)

def read_table(engine, table: str, columns="*", where: str|None=None) -> pd.DataFrame:
    q = f"SELECT {columns} FROM {table}"
    if where:
        q += f" WHERE {where}"
    return pd.read_sql(q, engine)
