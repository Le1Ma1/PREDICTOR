```
PREDICTOR/
├── .venv/                      # 本地虛擬環境 (不進版本控制)
├── .env                        # 環境變數 (API key, Supabase URL 等)
├── .gitignore                  # 忽略清單 (排除 .venv / .env / cache)
├── collect_me.py               # 收數據腳本 (抓取 API → Supabase Storage)
├── Makefile                    # 常用命令 (init-db / run-collect / run-clean)
├── pyproject.toml              # Python 專案設定 (Poetry/PDM/UV 等)
├── requirements.txt            # 套件清單
├── sql/
│ └── phase0_schema.sql         # Phase0 建表 SQL (六張表)
└── src/
├── clean/
│ ├── clean_liq_agg.py          # 清洗爆倉數據 → liq_agg_6h
│ └── clean_taker_flow.py       # 清洗 Taker Flow → taker_flow_6h
├── etl/ # (預留) ETL pipeline
└── load/ # (預留) DB 匯入模組
```