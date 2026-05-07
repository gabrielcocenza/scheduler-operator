"""Scheduler charm – ops charm class.

Relation interface (``cron``):

    Each relation carries exactly one job (one relation per job).

    Consumer **application** databag (written by the consumer leader):
        job-name:    the logical name of the scheduled job (string)
        cron:        standard five-field cron expression (string)
        <job-name>:  "done" | "retry"  – consumer's decision after each trigger
        ack-<job>:   the trigger timestamp the consumer is responding to;
                     used by the consumer to detect which jobs have newly fired

    Scheduler **application** databag (written by the APScheduler daemon via
    ``relation-set --app``, or by this charm when re-triggering on retry):
        trigger-<job>:  ISO-8601 UTC timestamp; a new value causes Juju to
                        dispatch ``cron-relation-changed`` on the consumer

Using application databags ensures a single consistent view regardless of
how many units each application has. Only the leader unit may write to the
application databag.

The scheduler never limits retries. The consumer decides when it is done.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import ops
from charmlibs.systemd import daemon_reload, service_enable, service_restart

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
        self.framework.observe(self.on.cron_relation_broken, self._on_cron_relation_broken)

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
        daemon_reload()

    def _enable_service(self) -> None:
        service_enable("--now", SERVICE_NAME)

    # ------------------------------------------------------------------
    # Config changed
    # ------------------------------------------------------------------

    def _on_config_changed(self, _event: ops.ConfigChangedEvent) -> None:
        self.unit.status = ops.MaintenanceStatus("Applying config")
        self._render_systemd_unit()
        service_restart(SERVICE_NAME)
        self.unit.status = ops.ActiveStatus("Ready")

    # ------------------------------------------------------------------
    # Relation events
    # ------------------------------------------------------------------

    def _on_cron_relation_changed(self, event: ops.RelationChangedEvent) -> None:
        """Handle relation data changes from the consumer application.

        Only the leader processes and responds to relation data changes.

        Two cases:
        1. Consumer published/updated ``job-name`` and ``cron`` keys → register the job.
        2. Consumer published a ``<job>: done | retry`` response → act accordingly.
        """
        if not self.unit.is_leader():
            return

        remote_app_data = event.relation.data[event.app]

        _METADATA_KEYS = frozenset({"job-name", "cron"})
        _JUJU_SYSTEM_KEYS = frozenset({"egress-subnets", "private-address", "ingress-address"})

        # --- Case 1: job-name / cron keys updated -----------------------
        job_name = remote_app_data.get("job-name")
        cron_expr = remote_app_data.get("cron")
        if job_name is not None and cron_expr is not None:
            self._update_jobs_for_app(event.relation, event.app, {job_name: cron_expr})
        elif job_name is None and cron_expr is None:
            pass  # no job metadata yet
        else:
            logger.warning(
                "Relation %s/%s has only one of job-name/cron set; skipping registration",
                event.relation.id, event.app.name,
            )

        # --- Case 2: job responses (done / retry) -----------------------
        # Consumer writes ``<job>: done|retry`` and ``ack-<job>: <timestamp>``.
        # Skip system keys, metadata keys, and "ack-*" acknowledgement keys.
        for key, value in remote_app_data.items():
            if key in _JUJU_SYSTEM_KEYS or key in _METADATA_KEYS or key.startswith("ack-"):
                continue
            job_name_resp = key
            if value == "retry":
                now = datetime.now(tz=timezone.utc).isoformat()
                event.relation.data[self.app][f"trigger-{job_name_resp}"] = now
                logger.info(
                    "Consumer requested retry for job %s on relation %s",
                    job_name_resp, event.relation.id,
                )
            elif value == "done":
                logger.info(
                    "Job %s completed on relation %s", job_name_resp, event.relation.id
                )

    def _on_cron_relation_broken(self, event: ops.RelationBrokenEvent) -> None:
        if not self.unit.is_leader():
            return
        self._remove_jobs_for_app(event.relation, event.app)

    # ------------------------------------------------------------------
    # jobs.json management
    # ------------------------------------------------------------------

    def _update_jobs_for_app(
        self,
        relation: ops.Relation,
        app: ops.Application,
        job_map: dict[str, str],
    ) -> None:
        all_jobs = _load_jobs_file()
        prefix = f"{relation.id}:{app.name}:"
        # Remove stale jobs for this app on this relation
        all_jobs = {k: v for k, v in all_jobs.items() if not k.startswith(prefix)}
        # Add/update
        for job_name, cron_expr in job_map.items():
            job_id = f"{relation.id}:{app.name}:{job_name}"
            all_jobs[job_id] = {"cron": cron_expr}
        self._write_jobs_file(all_jobs)
        service_restart(SERVICE_NAME)

    def _remove_jobs_for_app(self, relation: ops.Relation, app: ops.Application) -> None:
        all_jobs = _load_jobs_file()
        prefix = f"{relation.id}:{app.name}:"
        all_jobs = {k: v for k, v in all_jobs.items() if not k.startswith(prefix)}
        self._write_jobs_file(all_jobs)
        service_restart(SERVICE_NAME)

    @staticmethod
    def _write_jobs_file(jobs: dict) -> None:
        JOBS_FILE.parent.mkdir(parents=True, exist_ok=True)
        JOBS_FILE.write_text(json.dumps(jobs, indent=2))


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
