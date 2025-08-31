# src/train/train_logistic_6h.py
import argparse, yaml, joblib, numpy as np, pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from src.utils.paths import data_proc, artifacts
from src.utils.metrics import basic_scores, save_json
from src.utils.seed import set_seed

def time_cv_indices(n, n_splits=3, embargo=1):
    fold_sizes = np.full(n_splits, n // n_splits, dtype=int)
    fold_sizes[: n % n_splits] += 1
    idx = np.arange(n)
    bounds=[]; start=0
    for fs in fold_sizes:
        bounds.append((start, start+fs)); start += fs
    for i in range(n_splits):
        s,e = bounds[i]
        tr = np.r_[0:max(0,s-embargo), min(n,e+embargo):n]
        te = idx[s:e]
        yield tr, te

def main(cfg_path: str):
    cfg = yaml.safe_load(open(cfg_path, "r", encoding="utf-8"))
    set_seed(cfg["seed"])
    freq = cfg["freq"]; exp = cfg["exp_name"]

    # BTC 訓練集
    df_btc = pd.read_parquet(data_proc("BTC", freq) / "features.parquet")
    X_cols = [c for c in df_btc.columns if c.startswith(("ret_","hl_spread","rv_","oi_chg_","fund_","lsr_","liq_","taker_"))]
    X = df_btc[X_cols].values
    y = df_btc["y_up"].values

    # 時序 CV
    metrics_cv=[]; models=[]
    for k,(tr,te) in enumerate(time_cv_indices(len(df_btc), cfg["cv"]["n_splits"], cfg["cv"]["embargo"])):
        pipe = make_pipeline(
            StandardScaler(),
            LogisticRegression(
                max_iter=5000,
                C=1.0,
                class_weight="balanced",
                solver="lbfgs",
                n_jobs=None
            )
        )
        pipe.fit(X[tr], y[tr])
        proba = pipe.predict_proba(X[te])[:,1]
        metrics_cv.append(basic_scores(y[te], proba) | {"fold":k})
        models.append(pipe)

    art = artifacts("logit", exp)
    save_json(art / "metrics_cv.json", metrics_cv)
    joblib.dump(models[-1], art / "model.pkl")

    # ETH 測試集
    df_eth = pd.read_parquet(data_proc("ETH", freq) / "features.parquet")
    X_eth = df_eth[X_cols].values; y_eth = df_eth["y_up"].values
    proba_eth = models[-1].predict_proba(X_eth)[:,1]
    m_eth = basic_scores(y_eth, proba_eth)
    save_json(art / "metrics_eth_test.json", m_eth)
    pd.DataFrame({"ts_utc":df_eth["ts_utc"], "proba_up":proba_eth, "y":y_eth}) \
      .to_parquet(art / "preds_eth_test.parquet", index=False)
    print("LOGIT done.", m_eth)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--cfg", default="configs/exp_btc_train_eth_test.yaml")
    args = ap.parse_args()
    main(args.cfg)
