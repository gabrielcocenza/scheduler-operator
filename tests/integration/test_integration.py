"""Integration tests for the scheduler charm.

Tests run in file order with ``--exitfirst`` so a failure stops the suite.

Flow:
  1. Deploy both charms (scheduler + mock-cron-consumer).
  2. Integrate them via the ``cron`` relation.
  3. Wait for the scheduler APScheduler daemon to fire (``* * * * *`` = every minute).
  4. Assert both jobs (test-job-foo, test-job-bar) are triggered in the
     scheduler databag.
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_unit_relation_data(juju: jubilant.Juju, unit: str, endpoint: str) -> dict:
    """Return the local-unit relation databag for *unit* on *endpoint*."""
    raw = juju.cli("show-unit", unit, "--format", "json")
    unit_info = json.loads(raw).get(unit, {})
    for rel_info in unit_info.get("relation-info", []):
        if rel_info.get("endpoint") == endpoint:
            return rel_info.get("local-unit", {}).get("data", {})
    return {}


def _get_remote_unit_data(
    juju: jubilant.Juju, unit: str, endpoint: str, remote_unit: str
) -> dict:
    """Return the remote-unit relation databag as seen from *unit*."""
    raw = juju.cli("show-unit", unit, "--format", "json")
    unit_info = json.loads(raw).get(unit, {})
    for rel_info in unit_info.get("relation-info", []):
        if rel_info.get("endpoint") == endpoint:
            return rel_info.get("related-units", {}).get(remote_unit, {}).get("data", {})
    return {}


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
    """Integrate the charms and wait for idle."""
    juju.integrate(APP_SCHEDULER, APP_CONSUMER)
    juju.wait(
        jubilant.all_agents_idle,
        error=jubilant.any_error,
        timeout=120,
    )
    logger.info("Charms integrated and idle")


def test_cron_fires(juju: jubilant.Juju) -> None:
    """Wait for the scheduler daemon to fire and set trigger keys for all jobs."""

    def _all_triggers_set() -> bool:
        data = _get_unit_relation_data(juju, f"{APP_SCHEDULER}/0", "cron")
        return all(f"trigger-{job}" in data for job in JOBS)

    fired = _poll(_all_triggers_set, description="all trigger keys in scheduler databag")
    scheduler_data = _get_unit_relation_data(juju, f"{APP_SCHEDULER}/0", "cron")
    assert fired, (
        f"scheduler did not set all trigger keys within the timeout. "
        f"Current scheduler unit data: {scheduler_data}"
    )
    for job in JOBS:
        logger.info("trigger-%s = %s", job, scheduler_data.get(f"trigger-{job}"))


def test_consumer_responds(juju: jubilant.Juju) -> None:
    """Wait for the consumer to respond with <job>: done and ack-<job> for all jobs."""

    def _all_done() -> bool:
        data = _get_unit_relation_data(juju, f"{APP_CONSUMER}/0", "cron")
        return all(data.get(job) == "done" for job in JOBS)

    responded = _poll(_all_done, description="all jobs done in consumer databag")
    consumer_data = _get_unit_relation_data(juju, f"{APP_CONSUMER}/0", "cron")
    assert responded, (
        f"mock consumer did not report all jobs done within the timeout. "
        f"Current consumer unit data: {consumer_data}"
    )
    # Also verify the consumer wrote ack-<job> for each job (ack pattern)
    for job in JOBS:
        assert f"ack-{job}" in consumer_data, (
            f"consumer missing ack-{job} in databag: {consumer_data}"
        )
        logger.info("%s = %s, ack-%s = %s", job, consumer_data.get(job), job, consumer_data.get(f"ack-{job}"))

