"""
Binance WebSocket → Kafka Producer
====================================
Connects to Binance combined stream, receives real-time trade events
and publishes them to Kafka topics.

Usage:
    pip install confluent-kafka websocket-client python-dotenv
    python binance_producer.py
"""

import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone

import websocket
from confluent_kafka import Producer
from dotenv import load_dotenv
import os

load_dotenv()

# =============================================================
# Configuration
# =============================================================

SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    "client.id":         "binance-producer",
    # Wait for leader partition acknowledgement only (balance speed vs safety)
    "acks":              "1",
    # Batching settings for better throughput
    "linger.ms":         50,
    "batch.size":        16384,
}

BINANCE_WS_BASE    = "wss://stream.binance.com:9443/stream"
WHALE_THRESHOLD    = 50_000   # USD — trades above this are logged as whale alerts
LOG_EVERY_N_TRADES = 100      # print stats every N received trades

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
# Kafka producer
# =============================================================

producer = Producer(KAFKA_CONFIG)


def delivery_report(err, msg):
    """Callback invoked once per message after Kafka acknowledges delivery."""
    if err:
        log.error(f"Delivery failed | topic={msg.topic()} err={err}")


# =============================================================
# Trade parsing
# =============================================================

def parse_trade(raw: dict) -> dict:
    """
    Map a raw Binance payload to a clean, typed trade dict.

    Binance uses abbreviated keys to save bandwidth:
        e = event type       s  = symbol
        p = price            q  = quantity
        t = trade id         T  = trade timestamp (ms)
        m = is buyer maker   True means the SELLER initiated -> side = SELL
    """
    # Combined streams wrap payloads in {"stream": "...", "data": {...}}
    data = raw.get("data", raw)

    price    = float(data["p"])
    quantity = float(data["q"])
    is_buyer = not data["m"]   # invert: maker=True means sell-initiated

    return {
        "event_type":  data["e"],
        "symbol":      data["s"],
        "trade_id":    data["t"],
        "price":       price,
        "quantity":    quantity,
        "volume_usd":  round(price * quantity, 6),
        "side":        "BUY" if is_buyer else "SELL",
        "is_buyer":    is_buyer,
        "timestamp":   data["T"],                          # Unix ms
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================
# WebSocket handlers
# =============================================================

stats = {
    "received":   0,
    "errors":     0,
    "start_time": time.time(),
}


def on_message(ws, message):
    """Called for every incoming WebSocket frame."""
    try:
        raw   = json.loads(message)
        trade = parse_trade(raw)

        # One topic per symbol: btcusdt_trades, ethusdt_trades, ...
        topic = f"{trade['symbol'].lower()}_trades"

        producer.produce(
            topic    = topic,
            key      = str(trade["trade_id"]).encode(),
            value    = json.dumps(trade).encode(),
            callback = delivery_report,
        )
        # Give the Kafka client time to process callbacks without blocking
        producer.poll(0)

        stats["received"] += 1

        # Whale alert -- large trades often move the market
        if trade["volume_usd"] >= WHALE_THRESHOLD:
            log.warning(
                f"WHALE | {trade['symbol']} {trade['side']} "
                f"${trade['volume_usd']:>12,.2f} "
                f"@ ${trade['price']:,.2f}"
            )

        # Periodic stats log -- avoid flooding the console
        if stats["received"] % LOG_EVERY_N_TRADES == 0:
            elapsed = time.time() - stats["start_time"]
            rate    = stats["received"] / elapsed
            log.info(
                f"trades={stats['received']:,} | "
                f"rate={rate:.1f}/s | "
                f"last={trade['symbol']} ${trade['price']:,.2f} ({trade['side']})"
            )

    except (KeyError, ValueError, json.decoder.JSONDecodeError) as exc:
        stats["errors"] += 1
        log.warning(f"Parse error: {exc} | raw={message[:120]}")


def on_open(ws):
    log.info(f"Connected | symbols={[s.upper() for s in SYMBOLS]}")


def on_error(ws, error):
    log.error(f"WebSocket error: {error}")


def on_close(ws, code, msg):
    producer.flush()
    log.warning(
        f"Connection closed ({code}: {msg}) | "
        f"total={stats['received']:,} errors={stats['errors']}"
    )


# =============================================================
# Graceful shutdown
# =============================================================

def shutdown(signum, frame):
    log.info("Shutdown signal received -- flushing Kafka buffer...")
    producer.flush(timeout=10)
    sys.exit(0)


signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)


# =============================================================
# Entry point with auto-reconnect
# =============================================================

def build_ws_url(symbols: list[str]) -> str:
    """
    Build a Binance combined stream URL.
    A single connection handles all symbols -- far more efficient
    than one WebSocket per pair.

    Example:
        wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
    """
    streams = "/".join(f"{s}@trade" for s in symbols)
    return f"{BINANCE_WS_BASE}?streams={streams}"


def run():
    url            = build_ws_url(SYMBOLS)
    reconnect_wait = 5   # seconds between reconnect attempts

    log.info(f"Starting producer | url={url}")

    while True:
        ws = websocket.WebSocketApp(
            url,
            on_open    = on_open,
            on_message = on_message,
            on_error   = on_error,
            on_close   = on_close,
        )

        # Blocks until the connection drops
        ws.run_forever(
            ping_interval = 30,   # Binance requires a ping every 3 min; 30 s is safe
            ping_timeout  = 10,
        )

        log.warning(f"Disconnected -- reconnecting in {reconnect_wait}s...")
        time.sleep(reconnect_wait)

        # Exponential backoff, capped at 60 s
        reconnect_wait = min(reconnect_wait * 2, 60)


if __name__ == "__main__":
    run()