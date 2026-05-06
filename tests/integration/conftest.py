"""Integration test fixtures for the scheduler charm.

Builds both the scheduler charm and the mock-cron-consumer charm using
``charmcraft pack`` before the test session starts, and exposes their
``.charm`` file paths as session-scoped fixtures.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

import pytest

logger = logging.getLogger(__name__)

CHARM_ROOT = Path(__file__).parent.parent.parent  # repo root
MOCK_CHARM_DIR = Path(__file__).parent / "charm"


def _pack_charm(charm_dir: Path, label: str) -> Path:
    """Run ``charmcraft pack`` in *charm_dir* and return the packed ``.charm`` path."""
    logger.info("Packing %s charm in %s", label, charm_dir)
    result = subprocess.run(
        ["charmcraft", "pack", "--verbose"],
        cwd=charm_dir,
        capture_output=True,
        text=True,
        check=True,
    )
    logger.debug("charmcraft pack stdout:\n%s", result.stdout)

    charm_files = sorted(charm_dir.glob("*.charm"), key=lambda p: p.stat().st_mtime)
    if not charm_files:
        raise FileNotFoundError(
            f"No .charm file found in {charm_dir} after packing. "
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    charm_path = charm_files[-1]
    logger.info("Packed %s charm: %s", label, charm_path)
    return charm_path


@pytest.fixture(scope="session")
def scheduler_charm() -> Path:
    """Return the path to the packed scheduler .charm file."""
    return _pack_charm(CHARM_ROOT, "scheduler")


@pytest.fixture(scope="session")
def mock_consumer_charm() -> Path:
    """Return the path to the packed mock-cron-consumer .charm file."""
    return _pack_charm(MOCK_CHARM_DIR, "mock-cron-consumer")
