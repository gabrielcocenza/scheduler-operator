"""Unit tests for the scheduler charm.

Tests use ``ops.testing.Harness`` and do not require a live Juju environment.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

# Ensure src/ is importable
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import ops
import ops.testing

from charm import SchedulerCharm, JOBS_FILE, SERVICE_NAME


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _add_cron_relation(harness: ops.testing.Harness, remote_app: str = "mysql") -> tuple[int, str]:
    """Add a cron relation and return (relation_id, remote_app_name)."""
    rel_id = harness.add_relation("cron", remote_app)
    harness.add_relation_unit(rel_id, f"{remote_app}/0")
    return rel_id, remote_app


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
        self.harness.set_leader(True)
        self.harness.begin()

    def test_single_job_registered(self):
        rel_id, remote_app = _add_cron_relation(self.harness)
        with (
            patch.object(self.harness.charm, "_write_jobs_file") as mock_write,
            patch("charm.service_restart"),
        ):
            self.harness.update_relation_data(
                rel_id, remote_app, {"job-name": "backup", "cron": "0 2 * * *"}
            )
        written = mock_write.call_args[0][0]
        job_id = f"{rel_id}:mysql:backup"
        self.assertIn(job_id, written)
        self.assertEqual(written[job_id]["cron"], "0 2 * * *")

    def test_no_registration_when_only_one_metadata_key_present(self):
        """Only job-name without cron (or vice-versa) must not register a job."""
        rel_id, remote_app = _add_cron_relation(self.harness)
        with (
            patch.object(self.harness.charm, "_write_jobs_file") as mock_write,
            patch("charm.service_restart"),
        ):
            self.harness.update_relation_data(rel_id, remote_app, {"job-name": "backup"})
        mock_write.assert_not_called()

    def test_metadata_keys_not_treated_as_job_responses(self):
        """job-name and cron keys must never be processed as done/retry responses."""
        rel_id, remote_app = _add_cron_relation(self.harness)
        harness = self.harness
        harness.charm.model.get_relation("cron", rel_id).data[harness.charm.app].update({
            "trigger-backup": "2024-01-01T00:00:00+00:00",
        })
        initial_data = dict(harness.get_relation_data(rel_id, harness.charm.app.name))

        with (
            patch.object(harness.charm, "_write_jobs_file"),
            patch("charm.service_restart"),
        ):
            harness.update_relation_data(rel_id, remote_app, {
                "job-name": "backup",
                "cron": "0 2 * * *",
            })

        final_data = harness.get_relation_data(rel_id, harness.charm.app.name)
        self.assertEqual(initial_data.get("trigger-backup"), final_data.get("trigger-backup"))

    def test_non_leader_does_not_register_jobs(self):
        """Non-leader units must skip relation-changed processing entirely."""
        harness = ops.testing.Harness(SchedulerCharm)
        harness.set_leader(False)
        harness.begin()
        rel_id, remote_app = _add_cron_relation(harness)
        with (
            patch.object(harness.charm, "_write_jobs_file") as mock_write,
            patch("charm.service_restart"),
        ):
            harness.update_relation_data(rel_id, remote_app, {"job-name": "backup", "cron": "0 2 * * *"})
        mock_write.assert_not_called()

    def test_multiple_relations_tracked_separately(self):
        """Each relation registers its own job independently."""
        rel_id1, ra1 = _add_cron_relation(self.harness, "mysql")
        rel_id2, ra2 = _add_cron_relation(self.harness, "postgres")
        all_written: dict = {}

        def capture_write(jobs):
            all_written.update(jobs)

        with (
            patch.object(self.harness.charm, "_write_jobs_file", side_effect=capture_write),
            patch("charm.service_restart"),
        ):
            self.harness.update_relation_data(rel_id1, ra1, {"job-name": "backup", "cron": "0 2 * * *"})
            self.harness.update_relation_data(rel_id2, ra2, {"job-name": "clean", "cron": "0 4 * * *"})

        self.assertIn(f"{rel_id1}:mysql:backup", all_written)
        self.assertIn(f"{rel_id2}:postgres:clean", all_written)


# ---------------------------------------------------------------------------
# Relation broken – job removal
# ---------------------------------------------------------------------------

class TestCronRelationBroken(unittest.TestCase):
    def test_jobs_removed_on_relation_broken(self):
        harness = ops.testing.Harness(SchedulerCharm)
        harness.set_leader(True)
        harness.begin()
        rel_id, remote_app = _add_cron_relation(harness)

        remaining: list[dict] = []

        def capture_write(jobs):
            remaining.clear()
            remaining.append(jobs)

        with (
            patch.object(harness.charm, "_write_jobs_file", side_effect=capture_write),
            patch("charm.service_restart"),
        ):
            harness.update_relation_data(rel_id, remote_app, {"job-name": "backup", "cron": "0 2 * * *"})
            harness.remove_relation(rel_id)

        self.assertEqual(remaining[-1], {})


# ---------------------------------------------------------------------------
# Retry / job-response logic
# ---------------------------------------------------------------------------

class TestJobResponse(unittest.TestCase):
    def _setup(self):
        harness = ops.testing.Harness(SchedulerCharm)
        harness.set_leader(True)
        harness.begin()
        rel_id, remote_app = _add_cron_relation(harness)
        return harness, rel_id, remote_app

    def _seed_trigger(self, harness: ops.testing.Harness, rel_id: int, job_name: str = "backup") -> str:
        """Simulate the APScheduler daemon writing trigger-<job> to the app databag."""
        ts = "2024-01-01T00:00:00+00:00"
        harness.charm.model.get_relation("cron", rel_id).data[harness.charm.app].update({
            f"trigger-{job_name}": ts,
        })
        return ts

    def test_done_is_logged_without_scheduler_databag_change(self):
        harness, rel_id, remote_app = self._setup()
        ts = self._seed_trigger(harness, rel_id)
        initial_data = dict(harness.get_relation_data(rel_id, harness.charm.app.name))

        with (
            patch.object(harness.charm, "_write_jobs_file"),
            patch("charm.service_restart"),
        ):
            harness.update_relation_data(rel_id, remote_app, {
                "backup": "done",
                "ack-backup": ts,
            })

        final_data = harness.get_relation_data(rel_id, harness.charm.app.name)
        self.assertEqual(initial_data, dict(final_data))

    def test_ack_keys_are_not_treated_as_job_responses(self):
        harness, rel_id, remote_app = self._setup()
        self._seed_trigger(harness, rel_id)

        with (
            patch.object(harness.charm, "_write_jobs_file"),
            patch("charm.service_restart"),
        ):
            harness.update_relation_data(rel_id, remote_app, {
                "ack-backup": "2024-01-01T00:00:00+00:00",
            })

        our_data = harness.get_relation_data(rel_id, harness.charm.app.name)
        self.assertEqual(our_data.get("trigger-backup"), "2024-01-01T00:00:00+00:00")

    def test_retry_updates_trigger_timestamp(self):
        harness, rel_id, remote_app = self._setup()
        ts = self._seed_trigger(harness, rel_id)

        with (
            patch.object(harness.charm, "_write_jobs_file"),
            patch("charm.service_restart"),
        ):
            harness.update_relation_data(rel_id, remote_app, {
                "backup": "retry",
                "ack-backup": ts,
            })

        our_data = harness.get_relation_data(rel_id, harness.charm.app.name)
        self.assertIn("trigger-backup", our_data)
        self.assertNotEqual(our_data.get("trigger-backup"), ts)

    def test_retry_and_done_on_separate_relations(self):
        """Two jobs on separate relations: retry on one does not affect the other."""
        harness = ops.testing.Harness(SchedulerCharm)
        harness.set_leader(True)
        harness.begin()
        rel_id1, ra1 = _add_cron_relation(harness, "mysql")
        rel_id2, ra2 = _add_cron_relation(harness, "postgres")

        ts = "2024-01-01T00:00:00+00:00"
        harness.charm.model.get_relation("cron", rel_id1).data[harness.charm.app].update({"trigger-backup": ts})
        harness.charm.model.get_relation("cron", rel_id2).data[harness.charm.app].update({"trigger-vacuum": ts})

        with (
            patch.object(harness.charm, "_write_jobs_file"),
            patch("charm.service_restart"),
        ):
            harness.update_relation_data(rel_id1, ra1, {"backup": "retry", "ack-backup": ts})
            harness.update_relation_data(rel_id2, ra2, {"vacuum": "done", "ack-vacuum": ts})

        data1 = harness.get_relation_data(rel_id1, harness.charm.app.name)
        data2 = harness.get_relation_data(rel_id2, harness.charm.app.name)
        self.assertNotEqual(data1.get("trigger-backup"), ts)
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
        data = {"1:mysql:backup": {"cron": "0 2 * * *"}}
        p.write_text(json.dumps(data))
        result = _load_jobs(p)
        self.assertEqual(result, data)
        p.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
