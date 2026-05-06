"""Scheduler charm – ops charm class.

Relation interface (``cron``):

    Consumer unit databag (written by the consuming charm):
        jobs:        JSON dict of {job-name: cron-expression}
        <job>:       "done" | "retry"  – consumer's decision after each trigger
        ack-<job>:   the trigger timestamp the consumer is responding to;
                     used by the consumer to detect which jobs have newly fired

    Scheduler unit databag (written by the APScheduler daemon via
    ``relation-set``, or by this charm when re-triggering on retry):
        trigger-<job>:  ISO-8601 UTC timestamp; a new value causes Juju to
                        dispatch ``cron-relation-changed`` on the consumer

The scheduler never limits retries. The consumer decides when it is done.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import ops

logger = logging.getLogger(__name__)

JOBS_FILE = Path("/var/lib/juju/scheduler-jobs.json")
SERVICE_NAME = "juju-scheduler"
SYSTEMD_UNIT = f"/etc/systemd/system/{SERVICE_NAME}.service"


class SchedulerCharm(ops.CharmBase):

    def __init__(self, *args):
        super().__init__(*args)

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.cron_relation_changed, self._on_cron_relation_changed)
        self.framework.observe(self.on.cron_relation_departed, self._on_cron_relation_departed)

    # ------------------------------------------------------------------
    # Install
    # ------------------------------------------------------------------

    def _on_install(self, _event: ops.InstallEvent) -> None:
        self.unit.status = ops.MaintenanceStatus("Installing dependencies")
        self._write_jobs_file({})
        self._render_systemd_unit()
        self._enable_service()
        self.unit.status = ops.ActiveStatus("Ready")

    def _render_systemd_unit(self) -> None:
        charm_dir = os.environ.get("CHARM_DIR", str(Path(__file__).parent.parent))
        unit_name = os.environ.get("JUJU_UNIT_NAME", "scheduler/0")
        timezone = self.config.get("timezone", "UTC")
        service_py = str(Path(charm_dir) / "src" / "scheduler_service.py")

        template_path = Path(charm_dir) / "src"  / "templates" / "scheduler.service.j2"
        template = template_path.read_text()

        rendered = (
            template
            .replace("{{ unit_name }}", unit_name)
            .replace("{{ charm_dir }}", charm_dir)
            .replace("{{ jobs_file }}", str(JOBS_FILE))
            .replace("{{ service_py }}", service_py)
            .replace("{{ timezone }}", timezone)
        )
        Path(SYSTEMD_UNIT).write_text(rendered)
        subprocess.check_call(["systemctl", "daemon-reload"])

    def _enable_service(self) -> None:
        subprocess.check_call(["systemctl", "enable", "--now", SERVICE_NAME])

    # ------------------------------------------------------------------
    # Config changed
    # ------------------------------------------------------------------

    def _on_config_changed(self, _event: ops.ConfigChangedEvent) -> None:
        self.unit.status = ops.MaintenanceStatus("Applying config")
        self._render_systemd_unit()
        self._sighup_daemon()
        self.unit.status = ops.ActiveStatus("Ready")

    # ------------------------------------------------------------------
    # Relation events
    # ------------------------------------------------------------------

    def _on_cron_relation_changed(self, event: ops.RelationChangedEvent) -> None:
        """Handle relation data changes from the related unit.

        Two cases:
        1. Related unit published/updated its ``jobs`` key → register new jobs.
        2. Related unit published a ``<job>: done | retry`` response → act accordingly.
        """
        remote_unit = event.unit
        if remote_unit is None:
            return

        remote_data = event.relation.data[remote_unit]

        # --- Case 1: jobs key updated -----------------------------------
        raw_jobs = remote_data.get("jobs")
        if raw_jobs is not None:
            try:
                job_map = json.loads(raw_jobs)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON in 'jobs' from %s", remote_unit.name)
                job_map = {}
            self._update_jobs_for_unit(event.relation, remote_unit, job_map)

        # --- Case 2: job responses (done / retry) -----------------------
        # Consumer writes ``<job>: done|retry`` and ``ack-<job>: <timestamp>``.
        # Skip system keys, "jobs", and "ack-*" acknowledgement keys.
        _JUJU_SYSTEM_KEYS = frozenset({"egress-subnets", "private-address", "ingress-address"})
        for key, value in remote_data.items():
            if key in _JUJU_SYSTEM_KEYS or key == "jobs" or key.startswith("ack-"):
                continue
            job_name = key
            if value == "retry":
                now = datetime.now(tz=timezone.utc).isoformat()
                event.relation.data[self.unit][f"trigger-{job_name}"] = now
                logger.info(
                    "Consumer requested retry for job %s on relation %s",
                    job_name, event.relation.id,
                )
            elif value == "done":
                logger.info(
                    "Job %s completed on relation %s", job_name, event.relation.id
                )

    def _on_cron_relation_departed(self, event: ops.RelationDepartedEvent) -> None:
        if event.departing_unit is None:
            return
        self._remove_jobs_for_unit(event.relation, event.departing_unit)

    # ------------------------------------------------------------------
    # jobs.json management
    # ------------------------------------------------------------------

    def _update_jobs_for_unit(
        self,
        relation: ops.Relation,
        unit: ops.Unit,
        job_map: dict[str, str],
    ) -> None:
        all_jobs = _load_jobs_file()
        prefix = f"{relation.id}:{unit.name}:"
        # Remove stale jobs for this unit
        all_jobs = {k: v for k, v in all_jobs.items() if not k.startswith(prefix)}
        # Add/update
        for job_name, cron_expr in job_map.items():
            job_id = f"{relation.id}:{unit.name}:{job_name}"
            all_jobs[job_id] = {"cron": cron_expr}
        self._write_jobs_file(all_jobs)
        self._sighup_daemon()

    def _remove_jobs_for_unit(self, relation: ops.Relation, unit: ops.Unit) -> None:
        all_jobs = _load_jobs_file()
        prefix = f"{relation.id}:{unit.name}:"
        all_jobs = {k: v for k, v in all_jobs.items() if not k.startswith(prefix)}
        self._write_jobs_file(all_jobs)
        self._sighup_daemon()

    @staticmethod
    def _write_jobs_file(jobs: dict) -> None:
        JOBS_FILE.parent.mkdir(parents=True, exist_ok=True)
        JOBS_FILE.write_text(json.dumps(jobs, indent=2))

    @staticmethod
    def _sighup_daemon() -> None:
        pid_file = Path(f"/run/{SERVICE_NAME}.pid")
        if not pid_file.exists():
            logger.debug("No PID file found for %s, skipping SIGHUP", SERVICE_NAME)
            return
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, signal.SIGHUP)
            logger.info("Sent SIGHUP to %s (pid %d)", SERVICE_NAME, pid)
        except (ValueError, ProcessLookupError, PermissionError) as exc:
            logger.warning("Could not send SIGHUP to daemon: %s", exc)


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _load_jobs_file() -> dict:
    try:
        return json.loads(JOBS_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


if __name__ == "__main__":
    ops.main(SchedulerCharm)
