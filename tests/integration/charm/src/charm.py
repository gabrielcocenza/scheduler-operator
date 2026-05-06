"""Mock cron consumer charm – used in scheduler integration tests.

Registers two jobs, each on its own named endpoint:
  - ``cron-foo`` endpoint → job ``test-job-foo``
  - ``cron-bar`` endpoint → job ``test-job-bar``

Each endpoint uses ``interface: cron`` so Juju maps it to the scheduler's
``cron`` provide endpoint as a separate relation.

Protocol (``cron`` interface, one relation per job):
  - On ``<endpoint>-relation-joined``: publish ``job-name`` and ``cron`` for
    the single job assigned to that endpoint.
  - On ``<endpoint>-relation-changed``: check the scheduler's ``trigger-<job>``
    key. If it differs from the stored ``ack-<job>``, the job has newly fired;
    respond with ``<job>: done`` and ``ack-<job>: <same timestamp>``.

The ``ack-<job>`` key lets the consumer identify which triggers are new
without relying on which key caused the relation-changed event.
"""

from __future__ import annotations

import logging

import ops

logger = logging.getLogger(__name__)

CRON_EXPR = "* * * * *"
JOB_FOO = "test-job-foo"
JOB_BAR = "test-job-bar"


class MockCronConsumerCharm(ops.CharmBase):
    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.install, self._on_install)

        self.framework.observe(self.on.cron_foo_relation_joined, self._on_cron_foo_relation_joined)
        self.framework.observe(self.on.cron_foo_relation_changed, self._on_cron_foo_relation_changed)

        self.framework.observe(self.on.cron_bar_relation_joined, self._on_cron_bar_relation_joined)
        self.framework.observe(self.on.cron_bar_relation_changed, self._on_cron_bar_relation_changed)

    def _on_install(self, _event: ops.InstallEvent) -> None:
        self.unit.status = ops.ActiveStatus("Ready")

    # ------------------------------------------------------------------
    # cron-foo (test-job-foo)
    # ------------------------------------------------------------------

    def _on_cron_foo_relation_joined(self, event: ops.RelationJoinedEvent) -> None:
        event.relation.data[self.unit]["job-name"] = JOB_FOO
        event.relation.data[self.unit]["cron"] = CRON_EXPR
        logger.info("Registered job %s on cron-foo", JOB_FOO)
        self.unit.status = ops.ActiveStatus("Waiting for trigger")

    def _on_cron_foo_relation_changed(self, event: ops.RelationChangedEvent) -> None:
        self._handle_trigger(event, JOB_FOO)

    # ------------------------------------------------------------------
    # cron-bar (test-job-bar)
    # ------------------------------------------------------------------

    def _on_cron_bar_relation_joined(self, event: ops.RelationJoinedEvent) -> None:
        event.relation.data[self.unit]["job-name"] = JOB_BAR
        event.relation.data[self.unit]["cron"] = CRON_EXPR
        logger.info("Registered job %s on cron-bar", JOB_BAR)
        self.unit.status = ops.ActiveStatus("Waiting for trigger")

    def _on_cron_bar_relation_changed(self, event: ops.RelationChangedEvent) -> None:
        self._handle_trigger(event, JOB_BAR)

    # ------------------------------------------------------------------
    # Shared trigger handler
    # ------------------------------------------------------------------

    def _handle_trigger(self, event: ops.RelationChangedEvent, job_name: str) -> None:
        scheduler_units = [u for u in event.relation.units if "scheduler" in u.app.name]
        for sched_unit in scheduler_units:
            sched_data = event.relation.data[sched_unit]
            our_data = event.relation.data[self.unit]

            trigger_ts = sched_data.get(f"trigger-{job_name}")
            if trigger_ts is None:
                continue
            if our_data.get(f"ack-{job_name}") == trigger_ts:
                continue  # already processed this trigger

            logger.info("New trigger for job %s – reporting done", job_name)
            our_data[job_name] = "done"
            our_data[f"ack-{job_name}"] = trigger_ts

        self._update_status()

    def _update_status(self) -> None:
        foo_rel = self.model.get_relation("cron-foo")
        bar_rel = self.model.get_relation("cron-bar")
        foo_done = foo_rel is not None and foo_rel.data[self.unit].get(JOB_FOO) == "done"
        bar_done = bar_rel is not None and bar_rel.data[self.unit].get(JOB_BAR) == "done"
        if foo_done and bar_done:
            self.unit.status = ops.ActiveStatus("all jobs executed successfully")


if __name__ == "__main__":
    ops.main(MockCronConsumerCharm)
