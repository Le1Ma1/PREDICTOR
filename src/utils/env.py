import os
from dotenv import load_dotenv
from supabase import create_client

# 載入 .env
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "lake")
COINGLASS_API_KEY = os.getenv("CG_API_KEY")
EXCHANGE = os.getenv("EXCHANGE")
SYMBOL_PAIR = os.getenv("SYMBOL_PAIR")
SYMBOL_ASSET = os.getenv("SYMBOL_ASSET")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
