-- =============================================================
-- Gold Layer: mart_whale_trades
-- =============================================================
-- Identifies and analyses large trades ("whale activity").
-- A whale trade is any single trade above {{ var('whale_threshold') }} USD.
--
-- Provides:
--   - Individual whale trades with market context
--   - Hourly whale activity summary per symbol
--   - Whale buy/sell pressure indicator
--
-- Depends on: stg_trades (Silver)
-- =============================================================

with trades as (

    select * from {{ ref('stg_trades') }}

),

-- ----------------------------------------------------------
-- Hourly stats for context (all trades, not just whales)
-- ----------------------------------------------------------
hourly_market as (

    select
        symbol,
        date_trunc('hour', traded_at)   as hour,
        sum(volume_usd)                 as hour_total_volume_usd,
        count(*)                        as hour_total_trades,
        avg(price)                      as hour_avg_price
    from trades
    group by 1, 2

),

-- ----------------------------------------------------------
-- Whale trades — individual large transactions
-- ----------------------------------------------------------
whale_trades as (

    select
        t.trade_id,
        t.symbol,
        t.traded_at,
        t.price,
        t.quantity,
        t.volume_usd,
        t.side,

        -- Market context at the time of the whale trade
        date_trunc('hour', t.traded_at)                             as hour,
        h.hour_total_volume_usd,
        h.hour_total_trades,

        -- How much of the hour's volume this single trade represents
        round(
            t.volume_usd / nullif(h.hour_total_volume_usd, 0) * 100,
            4
        )                                                           as pct_of_hour_volume,

        -- Classify whale size
        case
            when t.volume_usd >= 1_000_000 then 'mega'    -- $1M+
            when t.volume_usd >= 500_000   then 'large'   -- $500K+
            when t.volume_usd >= 100_000   then 'medium'  -- $100K+
            else                                'small'   -- $50K–$100K
        end                                                         as whale_tier,

        -- Rank within the hour by volume (1 = largest)
        rank() over (
            partition by t.symbol, date_trunc('hour', t.traded_at)
            order by t.volume_usd desc
        )                                                           as rank_in_hour

    from trades t
    left join hourly_market h
        on  t.symbol = h.symbol
        and date_trunc('hour', t.traded_at) = h.hour
    where t.volume_usd >= {{ var('whale_threshold') }}

),

-- ----------------------------------------------------------
-- Hourly whale activity summary
-- ----------------------------------------------------------
hourly_whale_summary as (

    select
        symbol,
        hour,
        count(*)                                                    as whale_trade_count,
        round(sum(volume_usd), 2)                                   as whale_volume_usd,
        round(avg(volume_usd), 2)                                   as avg_whale_size_usd,
        round(max(volume_usd), 2)                                   as max_whale_size_usd,

        -- Net whale pressure: positive = whales buying
        round(
            sum(case when side = 'BUY' then volume_usd else -volume_usd end),
            2
        )                                                           as whale_net_pressure,

        count(*) filter (where side = 'BUY')                        as whale_buys,
        count(*) filter (where side = 'SELL')                       as whale_sells,

        count(*) filter (where whale_tier = 'mega')                 as mega_trades,
        count(*) filter (where whale_tier = 'large')                as large_trades,
        count(*) filter (where whale_tier = 'medium')               as medium_trades,
        count(*) filter (where whale_tier = 'small')                as small_trades

    from whale_trades
    group by 1, 2

)

-- Final output — individual whale trades enriched with hourly summary
select
    w.trade_id,
    w.symbol,
    w.traded_at,
    w.hour,
    w.price,
    w.quantity,
    w.volume_usd,
    w.side,
    w.whale_tier,
    w.pct_of_hour_volume,
    w.rank_in_hour,

    -- Hourly summary columns
    s.whale_trade_count                                             as hour_whale_count,
    s.whale_volume_usd                                              as hour_whale_volume,
    s.whale_net_pressure                                            as hour_net_pressure,
    s.mega_trades                                                   as hour_mega_count

from whale_trades w
left join hourly_whale_summary s
    on  w.symbol = s.symbol
    and w.hour   = s.hour
order by w.traded_at desc