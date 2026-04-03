# Binance Kafka Pipeline

A real-time cryptocurrency data pipeline built with **Apache Kafka**, **dbt**, and **DuckDB**, following the **Medallion architecture** (Bronze → Silver → Gold).

Trade events are streamed from the Binance WebSocket API, ingested into Kafka, persisted as Parquet files, transformed through dbt layers, and queried with DQL analytics.

---

## Architecture

```
Binance WebSocket API
    │   btcusdt@trade / ethusdt@trade / solusdt@trade
    ▼
Python Producer
    │   websocket-client → confluent-kafka
    ▼
Apache Kafka
    │   topics: btcusdt_trades / ethusdt_trades / solusdt_trades
    ▼
Python Consumer
    │   batches → Parquet (snappy) with hive partitioning
    ▼
Bronze Layer  data/bronze/trades/symbol=BTCUSDT/year=.../month=.../day=.../
    │
    ▼  dbt run
Silver Layer  stg_trades · stg_ohlcv_1min
    │         type casting · deduplication · OHLCV aggregation
    ▼  dbt run
Gold Layer    mart_vwap · mart_volatility · mart_whale_trades
    │         VWAP · Bollinger Bands · rolling volatility · CVD
    ▼
DuckDB / DQL Analytics
              ad-hoc queries · Jupyter notebooks
```

---

## Tech Stack

| Layer | Tool | Version |
|---|---|---------|
| Streaming | Apache Kafka | 7.6.0   |
| Producer / Consumer | Python | 3.11.9  |
| Kafka client | confluent-kafka | 2.13.2  |
| Serialization | pyarrow | 15.0.2  |
| Transformation | dbt-core | 1.10.1  |
| dbt adapter | dbt-duckdb | 1.16.3  |
| Analytics | DuckDB | 1.4.4   |
| Package manager | uv | latest  |
| Infrastructure | Docker Compose | —       |

---

## Project Structure

```
binance-kafka-pipeline/
│
├── docker-compose.yml          # Kafka + Zookeeper + Kafka UI
├── .env.example                # environment variable template
├── requirements.txt
│
├── producer/
│   └── binance_producer.py     # Binance WebSocket → Kafka
│
├── consumer/
│   └── kafka_consumer.py       # Kafka → Bronze Parquet
│
├── data/
│   ├── bronze/                 # raw Parquet files (gitignored)
│   ├── silver/                 # created by dbt
│   └── gold/                   # created by dbt
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml            # local only — see profiles.yml.example
│   └── models/
│       ├── staging/            # Silver layer
│       │   ├── sources.yml
│       │   ├── schema.yml
│       │   ├── stg_trades.sql
│       │   └── stg_ohlcv_1min.sql
│       └── marts/              # Gold layer
│           ├── schema.yml
│           ├── mart_vwap.sql
│           ├── mart_volatility.sql
│           └── mart_whale_trades.sql
│
└── analytics/
    ├── queries.sql             # DQL reference queries
    └── explore.ipynb           # Jupyter exploration notebook
```

---

## Getting Started

### Prerequisites

- Docker + Docker Compose
- Python 3.11.9
- [uv](https://github.com/astral-sh/uv)

### 1. Clone and install dependencies

```bash
git clone https://github.com/<your-username>/binance-kafka-pipeline.git
cd binance-kafka-pipeline

uv sync
```

### 2. Configure environment

```bash
cp .env.example .env
```

`.env.example`:
```
KAFKA_BOOTSTRAP=localhost:9092
BRONZE_PATH=data/bronze/trades
FLUSH_INTERVAL_SEC=30
FLUSH_BATCH_SIZE=500
```

### 3. Configure dbt profile

```bash
cp dbt_project/profiles.yml.example dbt_project/profiles.yml
```

`profiles.yml.example`:
```yaml
binance_pipeline:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "data/pipeline.duckdb"
      threads: 4
      extensions:
        - parquet
        - httpfs
```

### 4. Start Kafka

```bash
docker-compose up -d
```

Kafka UI is available at [http://localhost:8080](http://localhost:8080)

Wait for all topics to be created:
```bash
docker-compose logs kafka-init
```

### 5. Start the pipeline

Open two terminals:

```bash
# Terminal 1 — stream trades from Binance into Kafka
uv run python producer/binance_producer.py

# Terminal 2 — consume from Kafka and write to Parquet
uv run python consumer/kafka_consumer.py
```

After ~30 seconds the first Parquet files will appear under `data/bronze/`.

### 6. Run dbt transformations

```bash
cd dbt_project
uv run dbt run --profiles-dir .
uv run dbt test --profiles-dir .
```

### 7. Query with DuckDB

```bash
uv run python -c "import duckdb; duckdb.sql(\"SELECT * FROM read_parquet('data/bronze/trades/**/*.parquet', hive_partitioning=true) LIMIT 5\").show()"
```

Or open `analytics/explore.ipynb` in Jupyter.

---

## dbt Models

### Silver — `models/staging/`

| Model | Description |
|---|---|
| `stg_trades` | Cleaned and typed raw trades — deduplication, type casting, timestamp conversion |
| `stg_ohlcv_1min` | 1-minute OHLCV candles — open, high, low, close, volume, VWAP, volume delta |

### Gold — `models/marts/`

| Model | Description |
|---|---|
| `mart_vwap` | Hourly and rolling 30-minute VWAP per symbol |
| `mart_volatility` | Realized volatility, annualized volatility, Bollinger Bands |
| `mart_whale_trades` | Trades above $50k — whale activity by hour |

---

## Sample Queries

```sql
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
```

---

## Kafka Topics

| Topic | Partitions | Retention |
|---|---|---|
| `btcusdt_trades` | 3 | 7 days |
| `ethusdt_trades` | 3 | 7 days |
| `solusdt_trades` | 3 | 7 days |

---

## License

MIT
