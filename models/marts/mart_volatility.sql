-- =============================================================
-- Gold Layer: mart_volatility
-- =============================================================
-- Computes volatility metrics per symbol:
--   - Realized volatility (std dev of log returns)
--   - Annualized volatility
--   - Bollinger Bands (SMA-20 ± 2σ)
--   - %B indicator (price position within the bands)
--   - ATR — Average True Range
--
-- Depends on: stg_ohlcv_1min (Silver)
-- =============================================================

with candles as (

    select * from {{ ref('stg_ohlcv_1min') }}

),

-- ----------------------------------------------------------
-- Log returns — basis for all volatility calculations
-- ----------------------------------------------------------
returns as (

    select
        symbol,
        candle_time,
        open,
        high,
        low,
        close,
        volume,
        volume_usd,

        -- Log return: ln(close_t / close_t-1)
        -- More statistically stable than simple % return
        ln(
            close / nullif(
                lag(close) over (partition by symbol order by candle_time),
                0
            )
        )                                                           as log_return,

        -- True Range components for ATR
        high - low                                                  as hl_range,
        abs(high - lag(close) over (partition by symbol order by candle_time))
                                                                    as high_prev_close,
        abs(low  - lag(close) over (partition by symbol order by candle_time))
                                                                    as low_prev_close

    from candles

),

-- ----------------------------------------------------------
-- Bollinger Bands + rolling volatility (20-period window)
-- ----------------------------------------------------------
bands as (

    select
        symbol,
        candle_time,
        open,
        high,
        low,
        close,
        volume_usd,
        log_return,

        -- SMA-20
        round(
            avg(close) over w20,
            6
        )                                                           as sma_20,

        -- Standard deviation over 20 periods
        round(
            stddev(close) over w20,
            6
        )                                                           as std_20,

        -- Bollinger Bands: SMA ± 2σ
        round(avg(close) over w20 + 2 * stddev(close) over w20, 6) as bb_upper,
        round(avg(close) over w20 - 2 * stddev(close) over w20, 6) as bb_lower,

        -- Realized volatility: std dev of log returns over 20 periods
        round(
            stddev(log_return) over w20 * 100,
            6
        )                                                           as realized_vol_pct,

        -- ATR-14: average of true range over 14 periods
        round(
            avg(greatest(hl_range, high_prev_close, low_prev_close)) over w14,
            6
        )                                                           as atr_14,

        -- Bandwidth: (upper - lower) / sma — measures band squeeze
        round(
            (
                (avg(close) over w20 + 2 * stddev(close) over w20)
                - (avg(close) over w20 - 2 * stddev(close) over w20)
            ) / nullif(avg(close) over w20, 0),
            6
        )                                                           as bb_bandwidth

    from returns
    window
        w20 as (
            partition by symbol
            order by candle_time
            rows between 19 preceding and current row
        ),
        w14 as (
            partition by symbol
            order by candle_time
            rows between 13 preceding and current row
        )

)

select
    symbol,
    candle_time,
    close,
    sma_20,
    bb_upper,
    bb_lower,
    std_20,
    bb_bandwidth,
    realized_vol_pct,

    -- Annualized volatility: σ × √(525_600 minutes/year)
    round(realized_vol_pct * sqrt(525600) / 100, 6)                as annualized_vol,

    atr_14,

    -- %B: where is price within the bands?
    --   > 1.0 = above upper band (overbought signal)
    --   < 0.0 = below lower band (oversold signal)
    --   = 0.5 = exactly at SMA
    round(
        (close - bb_lower) / nullif(bb_upper - bb_lower, 0),
        6
    )                                                               as pct_b,

    log_return

from bands
where log_return is not null   -- drop the first row per symbol (no previous close)
order by symbol, candle_time desc