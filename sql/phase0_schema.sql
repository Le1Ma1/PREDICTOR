-- Price OHLCV (6h)
CREATE TABLE IF NOT EXISTS price_ohlcv_6h (
    time TIMESTAMPTZ PRIMARY KEY,  -- K 線時間
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume_usd DOUBLE PRECISION,
    _ts_utc TIMESTAMPTZ NOT NULL   -- 上傳時戳
);

-- Aggregated Open Interest (6h)
CREATE TABLE IF NOT EXISTS agg_oi_6h (
    time TIMESTAMPTZ PRIMARY KEY,
    oi_open DOUBLE PRECISION,
    oi_high DOUBLE PRECISION,
    oi_low DOUBLE PRECISION,
    oi_close DOUBLE PRECISION,
    _ts_utc TIMESTAMPTZ NOT NULL
);

-- Funding Rate (6h)
CREATE TABLE IF NOT EXISTS funding_6h (
    time TIMESTAMPTZ PRIMARY KEY,
    fund_open DOUBLE PRECISION,
    fund_high DOUBLE PRECISION,
    fund_low DOUBLE PRECISION,
    fund_close DOUBLE PRECISION,
    _ts_utc TIMESTAMPTZ NOT NULL
);

-- Long Short Ratio (6h)
CREATE TABLE IF NOT EXISTS lsr_6h (
    time TIMESTAMPTZ PRIMARY KEY,
    global_account_long_percent DOUBLE PRECISION,
    global_account_short_percent DOUBLE PRECISION,
    global_account_long_short_ratio DOUBLE PRECISION,
    _ts_utc TIMESTAMPTZ NOT NULL
);

-- Liquidation Aggregated (6h)
CREATE TABLE IF NOT EXISTS liq_agg_6h (
    time TIMESTAMPTZ PRIMARY KEY,
    long_liq_usd DOUBLE PRECISION,
    short_liq_usd DOUBLE PRECISION,
    _ts_utc TIMESTAMPTZ NOT NULL
);

-- Taker Flow (6h)
CREATE TABLE IF NOT EXISTS taker_flow_6h (
    time TIMESTAMPTZ PRIMARY KEY,
    buy_usd DOUBLE PRECISION,
    sell_usd DOUBLE PRECISION,
    _ts_utc TIMESTAMPTZ NOT NULL
);
