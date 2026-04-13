import logging
import subprocess
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

log = logging.getLogger(__name__)


def run_dbt():
    log.info("Running dbt ...")
    result = subprocess.run(
        ["uv", "run", "dbt", "run",
         "--profiles-dir", "dbt_project",
         "--project-dir", "dbt_project"
         ],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        log.info("dbt run succeeded")
    else:
        log.error(f"dbt run failed:\n{result.stderr}")


scheduler = BlockingScheduler()
scheduler.add_job(
    run_dbt,
    trigger=IntervalTrigger(minutes=5),
    id="dbt_run",
    next_run_time=datetime.now(),
)
scheduler.start()
