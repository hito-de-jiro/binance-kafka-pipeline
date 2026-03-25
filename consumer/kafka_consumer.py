"""
Kafka Consumer → Bronze Layer (Parquet)
=========================================
Reads trade events from Kafka topics and writes them to partitioned
Parquet files following the Medallion Bronze layer convention.

Output layout:
    data/bronze/trades/
        symbol=BTCUSDT/
            year=2024/month=01/day=15/
                trades_20240115_102300.parquet
        symbol=ETHUSDT/
            ...

Usage:
    pip install confluent-kafka pyarrow pandas python-dotenv
    python kafka_consumer.py
"""

import json
import logging
import os
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

load_dotenv()

# =============================================================
# Configuration
# =============================================================

TOPICS = ["btcusdt_trades", "ethusdt_trades", "solusdt_trades"]

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    "group.id": "bronze-consumer",
    "client.id": "bronze-consumer-01",
    "auto.offset.reset": "earliest",  # start from the beginning if no offset stored
    # Manual commit — we commit only after the file is safely written to disk
    "enable.auto.commit": False,
}

BRONZE_BASE_PATH = Path(os.getenv("BRONZE_PATH", "data/bronze/trades"))
FLUSH_INTERVAL_SEC = int(os.getenv("FLUSH_INTERVAL_SEC", "30"))  # write to disk every N seconds
FLUSH_BATCH_SIZE = int(os.getenv("FLUSH_BATCH_SIZE", "500"))  # or every N messages, whichever first
POLL_TIMEOUT_SEC = 1.0

# =============================================================
# Logging
# =============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# =============================================================
# Parquet schema
# =============================================================
# Explicit schema ensures consistent types across all files,
# even if Binance adds or removes fields in the future.

TRADE_SCHEMA = pa.schema([
    pa.field("event_type", pa.string()),
    pa.field("symbol", pa.string()),
    pa.field("trade_id", pa.int64()),
    pa.field("price", pa.float64()),
    pa.field("quantity", pa.float64()),
    pa.field("volume_usd", pa.float64()),
    pa.field("side", pa.string()),
    pa.field("is_buyer", pa.bool_()),
    pa.field("timestamp", pa.int64()),  # Unix ms — keep as-is for precision
    pa.field("ingested_at", pa.string()),  # ISO 8601 UTC string
])


# =============================================================
# Buffer — accumulates messages before flushing to Parquet
# =============================================================

class TradeBuffer:
    """
    In-memory buffer per symbol.
    Flushes to a Parquet file when either:
      - FLUSH_BATCH_SIZE messages are buffered, or
      - FLUSH_INTERVAL_SEC seconds have passed since last flush.
    """

    def __init__(self):
        # { "BTCUSDT": [trade_dict, ...] }
        self._buffer: dict[str, list[dict]] = defaultdict(list)
        self._last_flush = time.monotonic()
        self.files_written = 0
        self.rows_written = 0

    def add(self, trade: dict) -> bool:
        """
        Append a trade to the buffer.
        Returns True if a flush was triggered.
        """
        symbol = trade["symbol"]
        self._buffer[symbol].append(trade)

        total_buffered = sum(len(v) for v in self._buffer.values())
        elapsed = time.monotonic() - self._last_flush

        if total_buffered >= FLUSH_BATCH_SIZE or elapsed >= FLUSH_INTERVAL_SEC:
            self.flush()
            return True
        return False

    def flush(self, force: bool = False):
        """Write all buffered trades to Parquet files, then clear the buffer."""
        if not any(self._buffer.values()):
            return

        for symbol, rows in self._buffer.items():
            if not rows:
                continue
            self._write_parquet(symbol, rows)

        total = sum(len(v) for v in self._buffer.values())
        log.info(
            f"Flushed {total} rows across {len(self._buffer)} symbol(s) | "
            f"files_written={self.files_written} rows_written={self.rows_written}"
        )

        self._buffer.clear()
        self._last_flush = time.monotonic()

    def _write_parquet(self, symbol: str, rows: list[dict]):
        """
        Serialize rows to a Parquet file under the hive-partitioned path:
            <BRONZE_BASE_PATH>/symbol=<SYM>/year=YYYY/month=MM/day=DD/<file>.parquet
        """
        # Derive partition values from the first trade's timestamp
        ts_ms = rows[0]["timestamp"]
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")

        # Build the directory path
        partition_path = (
                BRONZE_BASE_PATH
                / f"symbol={symbol}"
                / f"year={year}"
                / f"month={month}"
                / f"day={day}"
        )
        partition_path.mkdir(parents=True, exist_ok=True)

        # Unique filename — timestamp prevents collisions when multiple
        # consumer instances run in parallel
        filename = f"trades_{dt.strftime('%Y%m%d_%H%M%S')}_{int(time.time() * 1000)}.parquet"
        file_path = partition_path / filename

        # Convert to Arrow table and write
        df = pd.DataFrame(rows)
        table = pa.Table.from_pandas(df, schema=TRADE_SCHEMA, preserve_index=False)

        pq.write_table(
            table,
            file_path,
            compression="snappy",  # fast compression, good ratio for numeric data
            row_group_size=len(rows),
        )

        self.files_written += 1
        self.rows_written += len(rows)

        log.debug(f"Wrote {len(rows)} rows -> {file_path}")


# =============================================================
# Consumer
# =============================================================

class BronzeConsumer:

    def __init__(self):
        self.consumer = Consumer(KAFKA_CONFIG)
        self.buffer = TradeBuffer()
        self.running = True
        self.stats = {"received": 0, "errors": 0, "start_time": time.time()}

    def start(self):
        self.consumer.subscribe(TOPICS, on_assign=self._on_assign)
        log.info(f"Subscribed to topics: {TOPICS}")

        try:
            self._poll_loop()
        except KafkaException as exc:
            log.error(f"Kafka exception: {exc}")
        finally:
            self._shutdown()

    def _poll_loop(self):
        while self.running:
            msg = self.consumer.poll(timeout=POLL_TIMEOUT_SEC)

            if msg is None:
                # No message within timeout — check if we should flush by time
                self.buffer.flush()
                continue

            if msg.error():
                self._handle_error(msg.error())
                continue

            self._process_message(msg)

    def _process_message(self, msg):
        try:
            trade = json.loads(msg.value().decode("utf-8"))
            flushed = self.buffer.add(trade)

            # Commit offset only after the file is safely on disk
            if flushed:
                self.consumer.commit(asynchronous=False)

            self.stats["received"] += 1

            if self.stats["received"] % 500 == 0:
                elapsed = time.time() - self.stats["start_time"]
                log.info(
                    f"received={self.stats['received']:,} | "
                    f"rate={self.stats['received'] / elapsed:.1f}/s | "
                    f"files={self.buffer.files_written} | "
                    f"rows={self.buffer.rows_written:,}"
                )

        except (json.JSONDecodeError, KeyError, pa.ArrowException) as exc:
            self.stats["errors"] += 1
            log.warning(f"Message processing error: {exc} | offset={msg.offset()}")

    def _handle_error(self, error):
        if error.code() == KafkaError._PARTITION_EOF:
            # Reached end of partition — not an error, just no new messages yet
            log.debug(f"Reached end of partition: {error}")
        else:
            log.error(f"Kafka error: {error}")
            self.stats["errors"] += 1

    def _on_assign(self, consumer, partitions):
        log.info(f"Partition assignment: {[str(p) for p in partitions]}")

    # ----------------------------------------------------------

    def _shutdown(self):
        log.info("Shutting down — flushing remaining buffer...")
        self.buffer.flush(force=True)

        # Final commit before closing
        self.consumer.commit(asynchronous=False)
        self.consumer.close()

        log.info(
            f"Consumer stopped | "
            f"received={self.stats['received']:,} | "
            f"errors={self.stats['errors']} | "
            f"files_written={self.buffer.files_written} | "
            f"rows_written={self.buffer.rows_written:,}"
        )

    def stop(self):
        self.running = False


# =============================================================
# Graceful shutdown
# =============================================================

consumer_instance: BronzeConsumer | None = None


def shutdown(signum, frame):
    log.info("Shutdown signal received...")
    if consumer_instance:
        consumer_instance.stop()


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# =============================================================
# Entry point
# =============================================================

if __name__ == "__main__":
    log.info(
        f"Starting Bronze consumer | "
        f"flush_interval={FLUSH_INTERVAL_SEC}s | "
        f"batch_size={FLUSH_BATCH_SIZE} | "
        f"output={BRONZE_BASE_PATH}"
    )

    consumer_instance = BronzeConsumer()
    consumer_instance.start()
