-- 六小時聚合爆倉
create table if not exists liq_agg_6h (
    _ts_utc timestamptz primary key,
    long_liq_usd numeric,
    short_liq_usd numeric
);

-- 六小時 Taker 主動買賣
create table if not exists taker_flow_6h (
    _ts_utc timestamptz primary key,
    buy_vol_usd numeric,
    sell_vol_usd numeric
);

-- 六小時價格 OHLCV
create table if not exists price_ohlcv_6h (
    _ts_utc timestamptz primary key,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric
);

-- 六小時聚合持倉量
create table if not exists agg_oi_6h (
    _ts_utc timestamptz primary key,
    open_interest_usd numeric
);

-- 六小時資金費率
create table if not exists funding_6h (
    _ts_utc timestamptz primary key,
    funding_rate numeric
);

-- 六小時全網多空比
create table if not exists lsr_6h (
    _ts_utc timestamptz primary key,
    long_ratio numeric,
    short_ratio numeric
);
