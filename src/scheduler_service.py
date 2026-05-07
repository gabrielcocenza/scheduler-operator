"""APScheduler daemon – runs as a systemd service.

This process is started by the scheduler charm's systemd unit. It:

1. Reads ``jobs.json`` to build the set of APScheduler jobs.
2. Listens for SIGHUP to reload ``jobs.json`` without restarting.
3. When a job fires it writes trigger keys directly to the scheduler unit's
   relation databag via ``juju-exec -- relation-set``, which causes Juju to
   dispatch ``cron-relation-changed`` on the related consumer unit.

Environment variables expected (written by the systemd unit):
    JUJU_UNIT_NAME   – e.g. "scheduler/0"
    JOBS_FILE        – path to jobs.json written by the ops charm
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("scheduler-service")

JUJU_EXEC = "/usr/bin/juju-exec"


def _load_jobs(jobs_file: Path) -> dict:
    """Return the jobs dict from *jobs_file*, or {} on any error."""
    try:
        return json.loads(jobs_file.read_text())
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.warning("Could not read %s: %s", jobs_file, exc)
        return {}


def _set_trigger(unit_name: str, relation_id: str, job_name: str) -> None:
    """Write a trigger key to the scheduler's application relation databag via relation-set.

    Using ``--app`` writes to the application databag so the trigger is visible
    regardless of which scheduler unit a consumer queries. Only the leader unit
    is permitted to write to the application databag; relation-set will fail
    with a non-zero exit code on non-leader units, which is handled gracefully.

    Changing ``trigger-<job>`` causes Juju to dispatch ``cron-relation-changed``
    on the consumer unit, which should then run the job and respond with
    ``<job>: done`` or ``<job>: retry``.
    """
    now = datetime.now(tz=timezone.utc).isoformat()
    cmd = [
        JUJU_EXEC,
        "-u",
        unit_name,
        f"relation-set --app -r {relation_id} trigger-{job_name}={now}",
    ]
    logger.info("Setting trigger for relation %s job %s", relation_id, job_name)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            logger.error(
                "relation-set failed (rc=%d): %s",
                result.returncode,
                result.stderr.strip(),
            )
        else:
            logger.info("Trigger set for relation %s job %s", relation_id, job_name)
    except subprocess.TimeoutExpired:
        logger.error("relation-set timed out for relation %s job %s", relation_id, job_name)
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error running relation-set: %s", exc)


def _sync_jobs(
    scheduler: BackgroundScheduler,
    jobs: dict,
    unit_name: str,
    timezone: str,
) -> None:
    """Reconcile APScheduler jobs against the *jobs* dict.

    The jobs dict has the structure produced by ``charm.py``:
    {
        "<relation_id>:<remote_unit>:<job_name>": {
            "cron": "0 2 * * *",
        },
        ...
    }
    """
    desired_ids = set(jobs.keys())
    existing_ids = {job.id for job in scheduler.get_jobs()}

    for job_id in existing_ids - desired_ids:
        logger.info("Removing job %s", job_id)
        scheduler.remove_job(job_id)

    for job_id, spec in jobs.items():
        relation_id, _remote_unit, job_name = job_id.split(":", 2)
        cron_expr = spec.get("cron", "")
        try:
            fields = cron_expr.split()
            if len(fields) != 5:
                raise ValueError("Expected 5 cron fields")
            minute, hour, day, month, day_of_week = fields
            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                timezone=timezone,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Invalid cron expression %r for job %s: %s", cron_expr, job_id, exc)
            continue

        kwargs = dict(
            unit_name=unit_name,
            relation_id=relation_id,
            job_name=job_name,
        )
        if job_id in existing_ids:
            scheduler.reschedule_job(job_id, trigger=trigger)
            logger.info("Rescheduled job %s (%s)", job_id, cron_expr)
        else:
            scheduler.add_job(
                _set_trigger,
                trigger=trigger,
                id=job_id,
                kwargs=kwargs,
                replace_existing=True,
                misfire_grace_time=60,
            )
            logger.info("Added job %s (%s)", job_id, cron_expr)


def main() -> None:  # pragma: no cover
    unit_name = os.environ.get("JUJU_UNIT_NAME", "")
    jobs_file = Path(os.environ.get("JOBS_FILE", "/var/lib/juju/scheduler-jobs.json"))
    timezone = os.environ.get("SCHEDULER_TIMEZONE", "UTC")

    if not unit_name:
        logger.error("JUJU_UNIT_NAME must be set")
        sys.exit(1)

    scheduler = BackgroundScheduler()
    reload_requested = False

    def _on_sighup(_signum, _frame):
        nonlocal reload_requested
        reload_requested = True

    signal.signal(signal.SIGHUP, _on_sighup)

    jobs = _load_jobs(jobs_file)
    _sync_jobs(scheduler, jobs, unit_name, timezone)
    scheduler.start()
    logger.info("APScheduler started with %d job(s)", len(jobs))

    try:
        while True:
            time.sleep(5)
            if reload_requested:
                reload_requested = False
                logger.info("SIGHUP received – reloading %s", jobs_file)
                jobs = _load_jobs(jobs_file)
                timezone = os.environ.get("SCHEDULER_TIMEZONE", "UTC")
                _sync_jobs(scheduler, jobs, unit_name, timezone)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down APScheduler")
        scheduler.shutdown()


if __name__ == "__main__":
    main()
