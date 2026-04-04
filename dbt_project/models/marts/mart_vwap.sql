-- =============================================================
-- Gold Layer: mart_vwap
-- =============================================================
-- Computes two VWAP variants per symbol:
--   1. Hourly VWAP    — one row per symbol per hour
--   2. Rolling VWAP   — 30-minute rolling window, per candle
--
-- Depends on: stg_ohlcv_1min (Silver)
-- =============================================================

with candles as (

    select * from {{ ref('stg_ohlcv_1min') }}

),

-- ----------------------------------------------------------
-- 1. Hourly VWAP
-- ----------------------------------------------------------
hourly_vwap as (

    select
        symbol,
        date_trunc('hour', candle_time)                             as hour,
        round(sum(volume_usd) / nullif(sum(volume), 0), 6)          as vwap,
        round(sum(volume_usd), 2)                                   as total_volume_usd,
        round(sum(volume), 8)                                       as total_volume,
        sum(trade_count)                                            as trade_count,
        max(high)                                                   as high,
        min(low)                                                    as low,
        first(open  order by candle_time)                           as open,
        last(close  order by candle_time)                           as close,
        round(max(high) - min(low), 6)                              as price_range
    from candles
    group by 1, 2

),

-- ----------------------------------------------------------
-- 2. Rolling 30-minute VWAP (per 1-min candle)
-- ----------------------------------------------------------
rolling_vwap as (

    select
        symbol,
        candle_time,
        close,
        volume,
        volume_usd,

        -- Rolling VWAP: sum(volume_usd) / sum(volume) over last 30 candles
        round(
            sum(volume_usd) over w30 / nullif(sum(volume) over w30, 0),
            6
        )                                                           as vwap_30min,

        -- Rolling VWAP over 60 minutes for trend comparison
        round(
            sum(volume_usd) over w60 / nullif(sum(volume) over w60, 0),
            6
        )                                                           as vwap_60min,

        -- Price position relative to VWAP (positive = price above VWAP)
        round(
            close - sum(volume_usd) over w30 / nullif(sum(volume) over w30, 0),
            6
        )                                                           as price_vs_vwap

    from candles
    window
        w30 as (
            partition by symbol
            order by candle_time
            rows between 29 preceding and current row
        ),
        w60 as (
            partition by symbol
            order by candle_time
            rows between 59 preceding and current row
        )

)

-- Final output: join both granularities
select
    r.symbol,
    r.candle_time,
    r.close,
    r.vwap_30min,
    r.vwap_60min,
    r.price_vs_vwap,
    h.vwap                                                          as hourly_vwap,
    h.high                                                          as hourly_high,
    h.low                                                           as hourly_low,
    h.total_volume_usd                                              as hourly_volume_usd,
    h.trade_count                                                   as hourly_trade_count
from rolling_vwap r
left join hourly_vwap h
    on  r.symbol = h.symbol
    and date_trunc('hour', r.candle_time) = h.hour
order by r.symbol, r.candle_time desc