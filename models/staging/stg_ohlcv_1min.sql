-- =============================================================
-- Silver Layer: stg_ohlcv_1min
-- =============================================================
-- Aggregates cleaned trades into 1-minute OHLCV candles.
-- This is the foundation for all Gold-layer metrics:
--   moving averages, Bollinger Bands, volatility, etc.
--
-- One row = one symbol + one minute.
-- =============================================================

with trades as (

    select * from {{ ref('stg_trades') }}

),

candles as (

    select
        symbol,
        date_trunc('minute', traded_at)                             as candle_time,

        -- OHLC
        first(price order by traded_at)                             as open,
        max(price)                                                  as high,
        min(price)                                                  as low,
        last(price  order by traded_at)                             as close,

        -- Volume
        round(sum(quantity), 8)                                     as volume,
        round(sum(volume_usd), 2)                                   as volume_usd,

        -- Trade counts
        count(*)                                                    as trade_count,
        count(*) filter (where side = 'BUY')                        as buy_count,
        count(*) filter (where side = 'SELL')                       as sell_count,

        -- Buy / sell volume split
        round(sum(quantity) filter (where side = 'BUY'),  8)        as buy_volume,
        round(sum(quantity) filter (where side = 'SELL'), 8)        as sell_volume,

        -- Volume delta: positive = buyers dominating
        round(
            sum(case when side = 'BUY' then quantity else -quantity end),
            8
        )                                                           as volume_delta,

        -- VWAP for this candle
        round(sum(price * quantity) / nullif(sum(quantity), 0), 6)  as vwap

    from trades
    group by 1, 2

)

select * from candles
order by symbol, candle_time