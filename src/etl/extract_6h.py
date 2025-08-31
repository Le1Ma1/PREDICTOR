import argparse, yaml, pandas as pd
from src.utils.db import get_engine, read_table
from src.utils.paths import data_raw

def merge_asset(engine, tables: dict, join_key: str) -> pd.DataFrame:
    price  = read_table(engine, tables["price"])
    agg_oi = read_table(engine, tables["agg_oi"])
    funding= read_table(engine, tables["funding"])
    lsr    = read_table(engine, tables["lsr"])
    liq    = read_table(engine, tables["liq"])
    taker  = read_table(engine, tables["taker"])

    for df in (price, agg_oi, funding, lsr, liq, taker):
        df[join_key] = pd.to_datetime(df[join_key], utc=True)

    price = price.rename(columns={
        "open":"px_open","high":"px_high","low":"px_low",
        "close":"px_close","volume_usd":"px_vol_usd"
    })
    agg_oi = agg_oi.rename(columns={"open":"oi_open","high":"oi_high","low":"oi_low","close":"oi_close"})
    funding = funding.rename(columns={"open":"fund_open","high":"fund_high","low":"fund_low","close":"fund_close"})
    lsr = lsr.rename(columns={"long_short_ratio":"lsr"})
    liq = liq.rename(columns={
        "long_liq_usd":"liq_long_usd","short_liq_usd":"liq_short_usd","total_liq_usd":"liq_total_usd"
    })
    taker = taker.rename(columns={
        "buy_volume_usd":"taker_buy_usd","sell_volume_usd":"taker_sell_usd","taker_ratio":"taker_ratio"
    })

    df = price.merge(agg_oi,  on=join_key, how="outer") \
              .merge(funding, on=join_key, how="outer") \
              .merge(lsr,     on=join_key, how="outer") \
              .merge(liq,     on=join_key, how="outer") \
              .merge(taker,   on=join_key, how="outer") \
              .sort_values(join_key).reset_index(drop=True)
    return df

def main(cfg_path: str, asset: str):
    cfg = yaml.safe_load(open(cfg_path, "r", encoding="utf-8"))
    eng = get_engine(cfg["env"]["SUPABASE_DB_URL"])
    join_key = cfg["columns"]["join_key"]
    tables = cfg["tables"][asset]
    out_dir = data_raw(asset, cfg["freq"])
    df = merge_asset(eng, tables, join_key)
    (out_dir / "raw.parquet").parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "raw.parquet", index=False)
    print(f"{asset} raw → {out_dir/'raw.parquet'} rows={len(df)}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--cfg", default="configs/exp_btc_train_eth_test.yaml")
    ap.add_argument("--asset", required=True, choices=["BTC","ETH"])
    args = ap.parse_args()
    main(args.cfg, args.asset)
