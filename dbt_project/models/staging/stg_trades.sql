-- =============================================================
-- Silver Layer: stg_trades
-- =============================================================
-- Cleans and types raw Bronze trades:
--   - casts all columns to correct types
--   - converts timestamp ms → proper timestamp
--   - casts ingested_at string → timestamp
--   - removes duplicates (same trade_id per symbol)
--   - filters out invalid rows (null price, zero quantity, etc.)
-- =============================================================

with source as (

    select * from {{ source('bronze', 'trades') }}

),

deduplicated as (

    -- Keep the first occurrence of each trade_id per symbol.
    -- Duplicates can appear when the consumer restarts and re-reads
    -- uncommitted Kafka offsets.
    select *
    from source
    qualify row_number() over (
        partition by symbol, trade_id
        order by ingested_at
    ) = 1

),

cleaned as (

    select
        -- identifiers
        cast(trade_id   as bigint)   as trade_id,
        cast(symbol     as varchar)  as symbol,

        -- price / volume
        cast(price      as double)   as price,
        cast(quantity   as double)   as quantity,
        round(cast(price as double)
            * cast(quantity as double), 6) as volume_usd,

        -- direction
        cast(side       as varchar)  as side,
        cast(is_buyer   as boolean)  as is_buyer,

        -- timestamps
        to_timestamp(
            cast(timestamp as bigint) / 1000.0
        )                            as traded_at,          -- proper UTC timestamp
        cast(timestamp as bigint)    as timestamp_ms,       -- keep raw ms for DuckDB window functions

        cast(ingested_at as timestamp) as ingested_at       -- was string in Bronze

    from deduplicated
    where
        price    > 0
        and quantity > 0
        and symbol   is not null
        and trade_id is not null

)

select * from cleaned