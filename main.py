"""
Binance Pipeline · Main Entry Point
=====================================
Starts and manages all pipeline components:
    - Binance WebSocket Producer
    - Kafka Consumer (Bronze layer writer)
    - APScheduler (dbt runs every 5 minutes)

Usage:
    uv run python main.py

Stop:
    Ctrl+C — gracefully shuts down all components
"""

import logging
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# =============================================================
# Paths
# =============================================================
ROOT = Path(__file__).parent
PRODUCER = ROOT / "producer" / "binance_producer.py"
CONSUMER = ROOT / "consumer" / "kafka_consumer.py"
DBT_PROJECT = ROOT / "dbt_project"

# =============================================================
# Logging
# =============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main")

# =============================================================
# Process registry
# =============================================================
processes: dict[str, subprocess.Popen] = {}


def check_kafka():
    """Verify Kafka is reachable before starting producer/consumer."""
    from confluent_kafka.admin import AdminClient
    try:
        client = AdminClient({"bootstrap.servers": "localhost:9092"})
        meta = client.list_topics(timeout=5)
        log.info(f"Kafka reachable | topics={list(meta.topics.keys())}")
    except Exception as e:
        log.error(f"Kafka not reachable: {e}")
        log.error("Run 'docker-compose up -d' first.")
        sys.exit(1)


def start_process(name: str, script: Path) -> subprocess.Popen:
    """Start a Python script as a subprocess and register it."""
    log.info(f"Starting {name}...")
    proc = subprocess.Popen(
        ["uv", "run", "python", str(script)],
        cwd=ROOT
    )
    processes[name] = proc
    log.info(f"{name} started | pid={proc.pid}")
    return proc


def stop_process(name: str):
    """Gracefully stop a registered subprocess."""
    proc = processes.get(name)
    if proc and proc.poll() is None:
        log.info(f"Stopping {name} (pid={proc.pid})...")
        proc.terminate()
        try:
            proc.wait(timeout=10)
            log.info(f"{name} stopped.")
        except subprocess.TimeoutExpired:
            log.warning(f"{name} did not stop — killing.")
            proc.kill()


def restart_process(name: str, script: Path):
    """Restart a subprocess if it has crashed."""
    stop_process(name)
    start_process(name, script)


def watch_process():
    """
    Check if any subprocess has crashed and restart it.
    Called by the scheduler every 60 seconds.
    """
    for name, script in [("producer", PRODUCER), ("consumer", CONSUMER), ]:
        proc = processes.get(name)
        if proc and proc.poll() is not None:
            log.warning(f"{name} crashed (exit={proc.returncode}) — restarting")
            restart_process(name, script)


# =============================================================
# dbt job
# =============================================================

def run_dbt():
    """Execute dbt run + dbt test. Called by the scheduler."""
    log.info("dbt run starting...")
    start = time.monotonic()

    result = subprocess.run(
        [
            "uv", "run", "dbt", "run",
            "--project-dir", str(DBT_PROJECT),
            "--profiles-dir", str(DBT_PROJECT),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True
    )

    elepsed = time.monotonic() - start

    if result.returncode == 0:
        log.info(f"dbt run succeeded in {elepsed:.1f}s.")
    else:
        log.error(f"dbt run failed in {elepsed:.1f}s:\n{result.stderr[-500:]}")
        return  # skip tests if run failed

    # Run tests only if models built successfully
    test_result = subprocess.run(
        [
            "uv", "run", "dbt", "test",
            "--project-dir", str(DBT_PROJECT),
            "--profiles-dir", str(DBT_PROJECT),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True
    )

    if test_result.returncode == 0:
        log.info("dbt test passed.")
    else:
        log.warning(f"dbt test failed:\n{test_result.stderr[-300:]}")


# =============================================================
# Scheduler
# =============================================================
def build_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler()

    # Run dbt every 5 minutes — trigger immediately on startup
    scheduler.add_job(
        run_dbt,
        trigger=IntervalTrigger(minutes=5),
        id="dbt_run",
        next_run_time=datetime.now(),
        max_instances=1,  # prevent overlapping runs
        coalesce=True,  # skip missed runs if previous is still running
    )

    # Watch for crashed processes every 60 seconds
    scheduler.add_job(
        watch_process,
        trigger=IntervalTrigger(seconds=60),
        id="process_watcher",
        next_run_time=datetime.now(),
    )

    return scheduler


# =============================================================
# Graceful shutdown
# =============================================================
scheduler: BackgroundScheduler | None = None


def shutdown(signum, frame):
    log.info("Shutdown signal received — stopping pipeline...")

    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
        log.info("Scheduler stopped.")

    for name in list(processes.keys()):
        stop_process(name)

    log.info("Pipeline stopped. Goodbye!")
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


# =============================================================
# Entry point
# =============================================================
def main():
    log.info("=" * 55)
    log.info(" Binance Kafka Pipeline")
    log.info("=" * 55)

    check_kafka()

    # 1. Start producer and consumer
    start_process(name="producer", script=PRODUCER)
    time.sleep(5)  # give producer time to connect before consumer starts
    start_process(name="consumer", script=CONSUMER)

    # 2. Start scheduler (dbt + process watcher)
    global scheduler
    scheduler = build_scheduler()
    scheduler.start()
    log.info("Scheduler started | dbt runs every 5 minutes.")

    log.info("=" * 55)
    log.info("Pipeline is running. Press Ctrl+C to stop.")
    log.info("=" * 55)

    # 3. Keep main thread alive — just monitor and log status
    while True:
        time.sleep(60)
        alive = [n for n, p in processes.items() if p.poll() is None]
        log.info(f"Status | alive={alive}")


if __name__ == "__main__":
    main()
