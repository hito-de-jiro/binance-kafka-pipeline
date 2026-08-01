"""
Kafka Stream Consumer → Live Metrics
======================================
Second consumer group alongside the Bronze writer.
Computes real-time VWAP / CVD / whale alerts per symbol and
persists them to SQLite for the Streamlit live panel.

    Kafka ──► bronze-consumer  → Parquet → dbt (batch)
         └──► live-metrics     → SQLite  → dashboard (stream)

Usage:
    uv run python consumer/stream_consumer.py
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sqlite3
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

load_dotenv()

# =============================================================
# Configuration
# =============================================================

TOPICS = ["btcusdt_trades", "ethusdt_trades", "solusdt_trades"]

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    "group.id": "live-metrics",
    "client.id": "live-metrics-01",
    "auto.offset.reset": "latest",  # live view — skip backlog
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5_000,
}

LIVE_DB_PATH = Path(os.getenv("LIVE_DB_PATH", "data/live/stream.db"))
WHALE_THRESHOLD = float(os.getenv("WHALE_THRESHOLD", "50000"))
SNAPSHOT_INTERVAL_SEC = float(os.getenv("SNAPSHOT_INTERVAL_SEC", "1.0"))
TICK_KEEP_SEC = int(os.getenv("TICK_KEEP_SEC", "1800"))  # 30 min of ticks
WHALE_KEEP = int(os.getenv("WHALE_KEEP", "200"))
POLL_TIMEOUT_SEC = 0.5

# Sliding windows (milliseconds)
WINDOWS_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "30m": 1_800_000,
}

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
# Sliding window
# =============================================================

@dataclass
class TradePoint:
    ts_ms: int
    price: float
    quantity: float
    volume_usd: float
    signed_usd: float  # +buy / -sell


@dataclass
class WindowStats:
    vwap: float | None = None
    volume_usd: float = 0.0
    buy_usd: float = 0.0
    sell_usd: float = 0.0
    cvd: float = 0.0
    trade_count: int = 0
    high: float | None = None
    low: float | None = None


class SlidingWindow:
    """Ring of trades pruned by wall-clock age."""

    def __init__(self, window_ms: int):
        self.window_ms = window_ms
        self._points: deque[TradePoint] = deque()
        self._sum_pq = 0.0   # Σ price * quantity
        self._sum_q = 0.0
        self._sum_usd = 0.0
        self._buy_usd = 0.0
        self._sell_usd = 0.0
        self._cvd = 0.0
        self._high: float | None = None
        self._low: float | None = None

    def add(self, point: TradePoint):
        self._points.append(point)
        self._sum_pq += point.price * point.quantity
        self._sum_q += point.quantity
        self._sum_usd += point.volume_usd
        self._cvd += point.signed_usd
        if point.signed_usd >= 0:
            self._buy_usd += point.volume_usd
        else:
            self._sell_usd += point.volume_usd
        self._high = point.price if self._high is None else max(self._high, point.price)
        self._low = point.price if self._low is None else min(self._low, point.price)
        self.prune(point.ts_ms)

    def prune(self, now_ms: int):
        cutoff = now_ms - self.window_ms
        while self._points and self._points[0].ts_ms < cutoff:
            old = self._points.popleft()
            self._sum_pq -= old.price * old.quantity
            self._sum_q -= old.quantity
            self._sum_usd -= old.volume_usd
            self._cvd -= old.signed_usd
            if old.signed_usd >= 0:
                self._buy_usd -= old.volume_usd
            else:
                self._sell_usd -= old.volume_usd
        # high/low may drift after prune — recompute only when needed
        if self._points and (self._high is None or self._low is None):
            self._recompute_extremes()
        elif not self._points:
            self._high = self._low = None
            self._sum_pq = self._sum_q = self._sum_usd = 0.0
            self._buy_usd = self._sell_usd = self._cvd = 0.0

    def _recompute_extremes(self):
        prices = [p.price for p in self._points]
        self._high = max(prices) if prices else None
        self._low = min(prices) if prices else None

    def stats(self, now_ms: int | None = None) -> WindowStats:
        if now_ms is not None:
            self.prune(now_ms)
        if not self._points:
            return WindowStats()
        # Extremes can be stale after prune; cheap check on size change is enough
        # for live UI — recompute when count is small.
        if len(self._points) <= 500:
            self._recompute_extremes()
        return WindowStats(
            vwap=round(self._sum_pq / self._sum_q, 6) if self._sum_q else None,
            volume_usd=round(self._sum_usd, 2),
            buy_usd=round(self._buy_usd, 2),
            sell_usd=round(self._sell_usd, 2),
            cvd=round(self._cvd, 2),
            trade_count=len(self._points),
            high=self._high,
            low=self._low,
        )


# =============================================================
# Per-symbol state
# =============================================================

@dataclass
class SymbolState:
    symbol: str
    last_price: float = 0.0
    last_side: str = ""
    last_trade_id: int = 0
    last_ts_ms: int = 0
    session_cvd: float = 0.0
    session_trades: int = 0
    session_volume_usd: float = 0.0
    windows: dict[str, SlidingWindow] = field(default_factory=dict)

    def __post_init__(self):
        if not self.windows:
            self.windows = {name: SlidingWindow(ms) for name, ms in WINDOWS_MS.items()}

    def update(self, trade: dict):
        price = float(trade["price"])
        qty = float(trade["quantity"])
        volume_usd = float(trade["volume_usd"])
        side = trade["side"]
        ts_ms = int(trade["timestamp"])
        signed = volume_usd if side == "BUY" else -volume_usd

        point = TradePoint(
            ts_ms=ts_ms,
            price=price,
            quantity=qty,
            volume_usd=volume_usd,
            signed_usd=signed,
        )

        self.last_price = price
        self.last_side = side
        self.last_trade_id = int(trade["trade_id"])
        self.last_ts_ms = ts_ms
        self.session_cvd += signed
        self.session_trades += 1
        self.session_volume_usd += volume_usd

        for window in self.windows.values():
            window.add(point)

    def snapshot(self) -> dict:
        now = self.last_ts_ms or int(time.time() * 1000)
        w = {name: win.stats(now) for name, win in self.windows.items()}
        w1 = w["1m"]
        w5 = w["5m"]
        w30 = w["30m"]

        price_vs_vwap_5m = None
        if w5.vwap is not None and self.last_price:
            price_vs_vwap_5m = round(self.last_price - w5.vwap, 6)

        return {
            "symbol": self.symbol,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "last_price": self.last_price,
            "last_side": self.last_side,
            "last_trade_id": self.last_trade_id,
            "last_ts_ms": self.last_ts_ms,
            "session_cvd": round(self.session_cvd, 2),
            "session_trades": self.session_trades,
            "session_volume_usd": round(self.session_volume_usd, 2),
            "vwap_1m": w1.vwap,
            "vwap_5m": w5.vwap,
            "vwap_30m": w30.vwap,
            "cvd_1m": w1.cvd,
            "cvd_5m": w5.cvd,
            "cvd_30m": w30.cvd,
            "volume_1m": w1.volume_usd,
            "volume_5m": w5.volume_usd,
            "trades_1m": w1.trade_count,
            "trades_5m": w5.trade_count,
            "high_5m": w5.high,
            "low_5m": w5.low,
            "price_vs_vwap_5m": price_vs_vwap_5m,
        }


# =============================================================
# SQLite store
# =============================================================

class LiveStore:
    """WAL SQLite — one writer (this process), many readers (dashboard)."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.con = sqlite3.connect(path, timeout=10, check_same_thread=False)
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def _init_schema(self):
        self.con.executescript(
            """
            CREATE TABLE IF NOT EXISTS metrics (
                symbol              TEXT PRIMARY KEY,
                updated_at          TEXT NOT NULL,
                last_price          REAL,
                last_side           TEXT,
                last_trade_id       INTEGER,
                last_ts_ms          INTEGER,
                session_cvd         REAL,
                session_trades      INTEGER,
                session_volume_usd  REAL,
                vwap_1m             REAL,
                vwap_5m             REAL,
                vwap_30m            REAL,
                cvd_1m              REAL,
                cvd_5m              REAL,
                cvd_30m             REAL,
                volume_1m           REAL,
                volume_5m           REAL,
                trades_1m           INTEGER,
                trades_5m           INTEGER,
                high_5m             REAL,
                low_5m              REAL,
                price_vs_vwap_5m    REAL
            );

            CREATE TABLE IF NOT EXISTS ticks (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT NOT NULL,
                ts_ms       INTEGER NOT NULL,
                price       REAL NOT NULL,
                vwap_5m     REAL,
                cvd_5m      REAL,
                session_cvd REAL,
                side        TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts
                ON ticks(symbol, ts_ms);

            CREATE TABLE IF NOT EXISTS whales (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id    INTEGER NOT NULL,
                symbol      TEXT NOT NULL,
                ts_ms       INTEGER NOT NULL,
                traded_at   TEXT NOT NULL,
                price       REAL NOT NULL,
                quantity    REAL NOT NULL,
                volume_usd  REAL NOT NULL,
                side        TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_whales_ts ON whales(ts_ms DESC);
            """
        )
        self.con.commit()

    def upsert_metrics(self, snap: dict):
        cols = list(snap.keys())
        placeholders = ", ".join("?" for _ in cols)
        col_names = ", ".join(cols)
        updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "symbol")
        self.con.execute(
            f"""
            INSERT INTO metrics ({col_names})
            VALUES ({placeholders})
            ON CONFLICT(symbol) DO UPDATE SET {updates}
            """,
            [snap[c] for c in cols],
        )

    def insert_tick(self, snap: dict):
        self.con.execute(
            """
            INSERT INTO ticks (symbol, ts_ms, price, vwap_5m, cvd_5m, session_cvd, side)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snap["symbol"],
                snap["last_ts_ms"],
                snap["last_price"],
                snap["vwap_5m"],
                snap["cvd_5m"],
                snap["session_cvd"],
                snap["last_side"],
            ),
        )

    def insert_whale(self, trade: dict):
        ts_ms = int(trade["timestamp"])
        traded_at = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
        self.con.execute(
            """
            INSERT INTO whales
                (trade_id, symbol, ts_ms, traded_at, price, quantity, volume_usd, side)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(trade["trade_id"]),
                trade["symbol"],
                ts_ms,
                traded_at,
                float(trade["price"]),
                float(trade["quantity"]),
                float(trade["volume_usd"]),
                trade["side"],
            ),
        )

    def prune(self, now_ms: int):
        cutoff = now_ms - TICK_KEEP_SEC * 1000
        self.con.execute("DELETE FROM ticks WHERE ts_ms < ?", (cutoff,))
        self.con.execute(
            """
            DELETE FROM whales WHERE id NOT IN (
                SELECT id FROM whales ORDER BY ts_ms DESC LIMIT ?
            )
            """,
            (WHALE_KEEP,),
        )

    def commit(self):
        self.con.commit()

    def close(self):
        self.con.commit()
        self.con.close()


# =============================================================
# Stream consumer
# =============================================================

class StreamConsumer:
    def __init__(self):
        self.consumer = Consumer(KAFKA_CONFIG)
        self.store = LiveStore(LIVE_DB_PATH)
        self.states: dict[str, SymbolState] = {}
        self.running = True
        self._last_snapshot = 0.0
        self._dirty = False
        self._pending_whales: list[dict] = []
        self.stats = {"received": 0, "whales": 0, "errors": 0, "start_time": time.time()}

    def start(self):
        self.consumer.subscribe(TOPICS, on_assign=self._on_assign)
        log.info(f"Subscribed to topics: {TOPICS}")
        log.info(f"Live DB: {LIVE_DB_PATH.resolve()}")

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
                self._maybe_flush()
                continue

            if msg.error():
                self._handle_error(msg.error())
                continue

            self._process_message(msg)
            self._maybe_flush()

    def _process_message(self, msg):
        try:
            trade = json.loads(msg.value().decode("utf-8"))
            symbol = trade["symbol"]

            state = self.states.get(symbol)
            if state is None:
                state = SymbolState(symbol=symbol)
                self.states[symbol] = state

            state.update(trade)
            self._dirty = True
            self.stats["received"] += 1

            if float(trade["volume_usd"]) >= WHALE_THRESHOLD:
                self._pending_whales.append(trade)
                self.stats["whales"] += 1
                log.warning(
                    f"WHALE | {symbol} {trade['side']} "
                    f"${float(trade['volume_usd']):>12,.2f} @ ${float(trade['price']):,.2f}"
                )

            if self.stats["received"] % 500 == 0:
                elapsed = time.time() - self.stats["start_time"]
                snap = state.snapshot()
                log.info(
                    f"received={self.stats['received']:,} | "
                    f"rate={self.stats['received'] / elapsed:.1f}/s | "
                    f"{symbol} ${snap['last_price']:,.2f} "
                    f"vwap5m={snap['vwap_5m']} cvd5m={snap['cvd_5m']:+,.0f}"
                )

        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            self.stats["errors"] += 1
            log.warning(f"Message processing error: {exc}")

    def _maybe_flush(self):
        now = time.monotonic()
        if not self._dirty and not self._pending_whales:
            return
        if now - self._last_snapshot < SNAPSHOT_INTERVAL_SEC:
            return

        now_ms = int(time.time() * 1000)
        for state in self.states.values():
            if state.session_trades == 0:
                continue
            snap = state.snapshot()
            self.store.upsert_metrics(snap)
            self.store.insert_tick(snap)

        for whale in self._pending_whales:
            self.store.insert_whale(whale)
        self._pending_whales.clear()

        self.store.prune(now_ms)
        self.store.commit()
        self._last_snapshot = now
        self._dirty = False

    def _handle_error(self, error):
        if error.code() == KafkaError._PARTITION_EOF:
            log.debug(f"EOF: {error}")
        else:
            log.error(f"Kafka error: {error}")
            self.stats["errors"] += 1

    def _on_assign(self, consumer, partitions):
        log.info(f"Partition assignment: {[str(p) for p in partitions]}")

    def _shutdown(self):
        log.info("Shutting down — flushing live metrics...")
        self._dirty = True
        self._last_snapshot = 0  # force flush
        self._maybe_flush()
        self.consumer.close()
        self.store.close()
        log.info(
            f"Stream consumer stopped | "
            f"received={self.stats['received']:,} | "
            f"whales={self.stats['whales']} | "
            f"errors={self.stats['errors']}"
        )

    def stop(self):
        self.running = False


# =============================================================
# Graceful shutdown
# =============================================================

consumer_instance: StreamConsumer | None = None


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
        f"Starting live stream consumer | "
        f"whale_threshold=${WHALE_THRESHOLD:,.0f} | "
        f"snapshot_interval={SNAPSHOT_INTERVAL_SEC}s | "
        f"db={LIVE_DB_PATH}"
    )
    consumer_instance = StreamConsumer()
    consumer_instance.start()
