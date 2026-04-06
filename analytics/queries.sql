-- Top 5 largest trades today
SELECT symbol, traded_at, price, volume_usd, side
FROM gold.mart_whale_trades
ORDER BY volume_usd DESC
LIMIT 5;

-- Rolling 30-minute VWAP for BTC
SELECT candle_time, vwap_30min, close
FROM gold.mart_vwap
WHERE symbol = 'BTCUSDT'
ORDER BY candle_time DESC
LIMIT 30;

-- Hourly volatility comparison
SELECT symbol, hour, volatility_pct, annualized_vol_pct
FROM gold.mart_volatility
ORDER BY hour DESC, symbol;