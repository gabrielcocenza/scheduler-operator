"""Unit tests for the scheduler charm.

Tests use ``ops.testing.Harness`` and do not require a live Juju environment.
"""

from __future__ import annotations

import signal
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

# Ensure src/ is importable
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import ops
import ops.testing

from charm import SchedulerCharm, JOBS_FILE, SERVICE_NAME


def _make_harness() -> ops.testing.Harness:
    harness = ops.testing.Harness(SchedulerCharm)
    harness.begin()
    return harness


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _patch_subprocess():
    return patch("charm.subprocess.check_call")


def _patch_systemd():
    return patch("charm.subprocess.check_call"), patch("charm.Path.write_text"), patch("charm.Path.read_text", return_value="")


def _add_cron_relation(harness: ops.testing.Harness, remote_app: str = "mysql") -> tuple[int, str]:
    rel_id = harness.add_relation("cron", remote_app)
    harness.add_relation_unit(rel_id, f"{remote_app}/0")
    return rel_id, f"{remote_app}/0"


# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------

class TestInstall(unittest.TestCase):
    @patch("charm.service_restart")
    @patch("charm.SchedulerCharm._enable_service")
    @patch("charm.SchedulerCharm._render_systemd_unit")
    def test_install_sets_active_status(self, _mock_render, _mock_enable, _mock_restart):
        harness = ops.testing.Harness(SchedulerCharm)
        with patch("charm.JOBS_FILE", new=Path("/tmp/test-jobs.json")):
            harness.begin_with_initial_hooks()
        self.assertIsInstance(harness.model.unit.status, ops.ActiveStatus)


# ---------------------------------------------------------------------------
# Relation joined / changed – job registration
# ---------------------------------------------------------------------------

class TestCronRelationChanged(unittest.TestCase):
    def setUp(self):
        self.harness = ops.testing.Harness(SchedulerCharm)
        self.harness.begin()

    def test_single_job_registered(self):
        rel_id, remote_unit = _add_cron_relation(self.harness)
        with (
            patch.object(self.harness.charm, "_write_jobs_file") as mock_write,
            patch("charm.service_restart"),
        ):
            self.harness.update_relation_data(
                rel_id, remote_unit, {"job-name": "backup", "cron": "0 2 * * *"}
            )
        written = mock_write.call_args[0][0]
        job_id = f"{rel_id}:mysql/0:backup"
        self.assertIn(job_id, written)
        self.assertEqual(written[job_id]["cron"], "0 2 * * *")

    def test_no_registration_when_only_one_metadata_key_present(self):
        """Only job-name without cron (or vice-versa) must not register a job."""
        rel_id, remote_unit = _add_cron_relation(self.harness)
        with (
            patch.object(self.harness.charm, "_write_jobs_file") as mock_write,
            patch("charm.service_restart"),
        ):
            self.harness.update_relation_data(rel_id, remote_unit, {"job-name": "backup"})
        # _write_jobs_file must not have been called (no valid job to register)
        mock_write.assert_not_called()

    def test_metadata_keys_not_treated_as_job_responses(self):
        """job-name and cron keys must never be processed as done/retry responses."""
        rel_id, remote_unit = _add_cron_relation(self.harness)
        # Seed a trigger so we have something in our databag to compare against
        harness = self.harness
        harness.charm.model.get_relation("cron", rel_id).data[harness.charm.unit].update({
            "trigger-backup": "2024-01-01T00:00:00+00:00",
        })
        initial_data = dict(harness.get_relation_data(rel_id, harness.charm.unit.name))

        with (
            patch.object(harness.charm, "_write_jobs_file"),
            patch("charm.service_restart"),
        ):
            harness.update_relation_data(rel_id, remote_unit, {
                "job-name": "backup",
                "cron": "0 2 * * *",
            })

        final_data = harness.get_relation_data(rel_id, harness.charm.unit.name)
        # Scheduler must not have written a new trigger-backup (metadata not a response)
        self.assertEqual(initial_data.get("trigger-backup"), final_data.get("trigger-backup"))

    def test_multiple_relations_tracked_separately(self):
        """Each relation registers its own job independently."""
        rel_id1, ru1 = _add_cron_relation(self.harness, "mysql")
        rel_id2, ru2 = _add_cron_relation(self.harness, "postgres")
        all_written: dict = {}

        def capture_write(jobs):
            all_written.update(jobs)

        with (
            patch.object(self.harness.charm, "_write_jobs_file", side_effect=capture_write),
            patch("charm.service_restart"),
        ):
            self.harness.update_relation_data(rel_id1, ru1, {"job-name": "backup", "cron": "0 2 * * *"})
            self.harness.update_relation_data(rel_id2, ru2, {"job-name": "clean", "cron": "0 4 * * *"})

        self.assertIn(f"{rel_id1}:mysql/0:backup", all_written)
        self.assertIn(f"{rel_id2}:postgres/0:clean", all_written)


# ---------------------------------------------------------------------------
# Relation departed
# ---------------------------------------------------------------------------

class TestCronRelationDeparted(unittest.TestCase):
    def test_jobs_removed_on_departed(self):
        harness = ops.testing.Harness(SchedulerCharm)
        harness.begin()
        rel_id, remote_unit = _add_cron_relation(harness)

        remaining: list[dict] = []

        def capture_write(jobs):
            remaining.clear()
            remaining.append(jobs)

        with (
            patch.object(harness.charm, "_write_jobs_file", side_effect=capture_write),
            patch("charm.service_restart"),
        ):
            harness.update_relation_data(rel_id, remote_unit, {"job-name": "backup", "cron": "0 2 * * *"})
            harness.remove_relation_unit(rel_id, remote_unit)

        self.assertEqual(remaining[-1], {})


# ---------------------------------------------------------------------------
# Retry / job-response logic
# ---------------------------------------------------------------------------

class TestJobResponse(unittest.TestCase):
    def _setup(self):
        harness = ops.testing.Harness(SchedulerCharm)
        harness.begin()
        rel_id, remote_unit = _add_cron_relation(harness)
        return harness, rel_id, remote_unit

    def _seed_trigger(self, harness: ops.testing.Harness, rel_id: int, job_name: str = "backup") -> str:
        """Simulate the APScheduler daemon writing trigger-<job> via relation-set."""
        ts = "2024-01-01T00:00:00+00:00"
        harness.charm.model.get_relation("cron", rel_id).data[harness.charm.unit].update({
            f"trigger-{job_name}": ts,
        })
        return ts

    def test_done_is_logged_without_scheduler_databag_change(self):
        harness, rel_id, remote_unit = self._setup()
        ts = self._seed_trigger(harness, rel_id)
        initial_data = dict(harness.get_relation_data(rel_id, harness.charm.unit.name))

        with (
            patch.object(harness.charm, "_write_jobs_file"),
            patch("charm.service_restart"),
        ):
            harness.update_relation_data(rel_id, remote_unit, {
                "backup": "done",
                "ack-backup": ts,
            })

        # Scheduler logs done but does not change its own databag
        final_data = harness.get_relation_data(rel_id, harness.charm.unit.name)
        self.assertEqual(initial_data, dict(final_data))

    def test_ack_keys_are_not_treated_as_job_responses(self):
        harness, rel_id, remote_unit = self._setup()
        self._seed_trigger(harness, rel_id)

        with (
            patch.object(harness.charm, "_write_jobs_file"),
            patch("charm.service_restart"),
        ):
            # Only send ack-backup, no response key
            harness.update_relation_data(rel_id, remote_unit, {
                "ack-backup": "2024-01-01T00:00:00+00:00",
            })

        # trigger-backup must be unchanged — ack key must not trigger retry
        our_data = harness.get_relation_data(rel_id, harness.charm.unit.name)
        self.assertEqual(our_data.get("trigger-backup"), "2024-01-01T00:00:00+00:00")

    def test_retry_updates_trigger_timestamp(self):
        harness, rel_id, remote_unit = self._setup()
        ts = self._seed_trigger(harness, rel_id)

        with (
            patch.object(harness.charm, "_write_jobs_file"),
            patch("charm.service_restart"),
        ):
            harness.update_relation_data(rel_id, remote_unit, {
                "backup": "retry",
                "ack-backup": ts,
            })

        our_data = harness.get_relation_data(rel_id, harness.charm.unit.name)
        # trigger-backup must now be a new timestamp (different from seed)
        self.assertIn("trigger-backup", our_data)
        self.assertNotEqual(our_data.get("trigger-backup"), ts)

    def test_retry_and_done_on_separate_relations(self):
        """Two jobs on separate relations: retry on one does not affect the other."""
        harness = ops.testing.Harness(SchedulerCharm)
        harness.begin()
        rel_id1, ru1 = _add_cron_relation(harness, "mysql")
        rel_id2, ru2 = _add_cron_relation(harness, "postgres")

        ts = "2024-01-01T00:00:00+00:00"
        harness.charm.model.get_relation("cron", rel_id1).data[harness.charm.unit].update({"trigger-backup": ts})
        harness.charm.model.get_relation("cron", rel_id2).data[harness.charm.unit].update({"trigger-vacuum": ts})

        with (
            patch.object(harness.charm, "_write_jobs_file"),
            patch("charm.service_restart"),
        ):
            harness.update_relation_data(rel_id1, ru1, {"backup": "retry", "ack-backup": ts})
            harness.update_relation_data(rel_id2, ru2, {"vacuum": "done", "ack-vacuum": ts})

        data1 = harness.get_relation_data(rel_id1, harness.charm.unit.name)
        data2 = harness.get_relation_data(rel_id2, harness.charm.unit.name)
        # backup was retried on rel1 — trigger must be updated
        self.assertNotEqual(data1.get("trigger-backup"), ts)
        # vacuum was done on rel2 — trigger must be unchanged
        self.assertEqual(data2.get("trigger-vacuum"), ts)


# ---------------------------------------------------------------------------
# Daemon helpers
# ---------------------------------------------------------------------------

class TestSchedulerService(unittest.TestCase):
    """Tests for scheduler_service.py logic (no actual scheduling)."""

    def test_load_jobs_missing_file_returns_empty(self):
        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        from scheduler_service import _load_jobs
        result = _load_jobs(Path("/tmp/nonexistent-scheduler-jobs-xyz.json"))
        self.assertEqual(result, {})

    def test_load_jobs_invalid_json_returns_empty(self):
        from scheduler_service import _load_jobs
        p = Path("/tmp/bad-jobs.json")
        p.write_text("not json")
        result = _load_jobs(p)
        self.assertEqual(result, {})
        p.unlink(missing_ok=True)

    def test_load_jobs_valid(self):
        from scheduler_service import _load_jobs
        import json
        p = Path("/tmp/good-jobs.json")
        data = {"1:mysql/0:backup": {"cron": "0 2 * * *"}}
        p.write_text(json.dumps(data))
        result = _load_jobs(p)
        self.assertEqual(result, data)
        p.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
