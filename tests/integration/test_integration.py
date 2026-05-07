"""Integration tests for the scheduler charm.

Tests run in file order with ``--exitfirst`` so a failure stops the suite.

Flow:
  1. Deploy both charms (scheduler + mock-cron-consumer).
  2. Integrate them via two separate ``cron`` relations — one per job:
       scheduler:cron ↔ mock-consumer:cron-foo  (test-job-foo)
       scheduler:cron ↔ mock-consumer:cron-bar  (test-job-bar)
  3. Wait for the scheduler APScheduler daemon to fire (``* * * * *`` = every minute).
  4. Assert both jobs (test-job-foo, test-job-bar) are triggered in the
     scheduler databag (one relation per job).
  5. Assert the consumer processes each job independently using the
     ``ack-<job>`` pattern and reports ``<job>: done`` for both.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import jubilant
import pytest

logger = logging.getLogger(__name__)

# Maximum time (seconds) to wait for a scheduled trigger to appear.
# With "* * * * *" the daemon fires within 60 s; we allow 150 s for slow CI.
TRIGGER_TIMEOUT = 150

APP_SCHEDULER = "scheduler"
APP_CONSUMER = "mock-consumer"

JOBS = ["test-job-foo", "test-job-bar"]

# Mapping: job name → consumer endpoint name
JOB_ENDPOINTS = {
    "test-job-foo": "cron-foo",
    "test-job-bar": "cron-bar",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_remote_app_data(
    juju: jubilant.Juju, unit: str, endpoint: str
) -> dict:
    """Return the **remote** application's relation databag as seen from *unit* on *endpoint*.

    In Juju's ``show-unit`` output, ``application-data`` under ``relation-info`` contains
    the *remote* application's databag (not the local one).  For example:
    - ``show-unit mock-consumer/0`` → ``application-data`` = scheduler's data
    - ``show-unit scheduler/0``     → ``application-data`` = consumer's data
    """
    raw = juju.cli("show-unit", unit, "--format", "json")
    unit_info = json.loads(raw).get(unit, {})
    for rel_info in unit_info.get("relation-info", []):
        if rel_info.get("endpoint") == endpoint:
            return rel_info.get("application-data", {})
    return {}


def _get_all_remote_app_data_by_endpoint(
    juju: jubilant.Juju, unit: str, endpoint: str
) -> list[dict]:
    """Return all remote-app databags for every relation on *endpoint* for *unit*.

    Useful when a unit has multiple relations on the same endpoint (e.g. ``scheduler/0``
    has two ``cron`` relations).
    """
    raw = juju.cli("show-unit", unit, "--format", "json")
    unit_info = json.loads(raw).get(unit, {})
    return [
        rel_info.get("application-data", {})
        for rel_info in unit_info.get("relation-info", [])
        if rel_info.get("endpoint") == endpoint
    ]


def _poll(
    condition,
    *,
    timeout: float = TRIGGER_TIMEOUT,
    interval: float = 5.0,
    description: str = "condition",
) -> bool:
    """Poll *condition()* every *interval* seconds until it returns True or *timeout* expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        remaining = deadline - time.monotonic()
        logger.info("Waiting for %s (%.0fs remaining)…", description, remaining)
        time.sleep(min(interval, max(0, remaining)))
    return False


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.juju_setup
def test_deploy(
    juju: jubilant.Juju,
    scheduler_charm: Path,
    mock_consumer_charm: Path,
) -> None:
    """Deploy both charms and wait for active status."""
    juju.deploy(scheduler_charm, APP_SCHEDULER)
    juju.deploy(mock_consumer_charm, APP_CONSUMER)
    juju.wait(
        jubilant.all_active,
        error=jubilant.any_error,
        timeout=1000,
    )
    logger.info("Both charms deployed and active")


def test_relate(juju: jubilant.Juju) -> None:
    """Integrate the charms with two separate relations, one per job."""
    juju.integrate(f"{APP_SCHEDULER}:cron", f"{APP_CONSUMER}:cron-foo")
    juju.integrate(f"{APP_SCHEDULER}:cron", f"{APP_CONSUMER}:cron-bar")
    juju.wait(
        jubilant.all_agents_idle,
        error=jubilant.any_error,
        timeout=120,
    )
    logger.info("Charms integrated (cron-foo and cron-bar) and idle")


def test_cron_fires(juju: jubilant.Juju) -> None:
    """Wait for the scheduler daemon to fire and set trigger keys for all jobs.

    Each job lives on its own relation. We check the scheduler's trigger keys by
    reading from the consumer side: ``show-unit mock-consumer/0`` exposes the
    scheduler's app databag under ``application-data`` for each consumer endpoint.
    """
    def _all_triggers_set() -> bool:
        triggered = set()
        for job, endpoint in JOB_ENDPOINTS.items():
            data = _get_remote_app_data(juju, f"{APP_CONSUMER}/0", endpoint)
            if f"trigger-{job}" in data:
                triggered.add(job)
        return triggered == set(JOBS)

    fired = _poll(_all_triggers_set, description="all trigger keys in scheduler app databag")
    assert fired, "Scheduler did not set all trigger keys within the timeout."
    for job in JOBS:
        logger.info("trigger-%s fired", job)


def test_consumer_responds(juju: jubilant.Juju) -> None:
    """Wait for the consumer to respond with <job>: done and ack-<job> on each relation.

    We read from ``scheduler/0`` because ``application-data`` from the scheduler's
    perspective contains the consumer's app databag. Each ``cron`` relation on the
    scheduler has a unique ``job-name`` key we use to identify which relation belongs
    to which job.
    """

    def _all_done() -> bool:
        all_rel_data = _get_all_remote_app_data_by_endpoint(
            juju, f"{APP_SCHEDULER}/0", "cron"
        )
        done_jobs = set()
        for rel_data in all_rel_data:
            job = rel_data.get("job-name")
            if job and rel_data.get(job) == "done":
                done_jobs.add(job)
        return done_jobs == set(JOBS)

    responded = _poll(_all_done, description="all jobs done in consumer databag")
    assert responded, "Mock consumer did not report all jobs done within the timeout."

    all_rel_data = _get_all_remote_app_data_by_endpoint(
        juju, f"{APP_SCHEDULER}/0", "cron"
    )
    rel_by_job = {d.get("job-name"): d for d in all_rel_data if d.get("job-name")}
    for job in JOBS:
        data = rel_by_job.get(job, {})
        assert data.get(job) == "done", f"consumer missing {job}=done"
        assert f"ack-{job}" in data, f"consumer missing ack-{job}"
        logger.info("[%s] %s=%s, ack-%s=%s", job, job, data.get(job), job, data.get(f"ack-{job}"))

