"""Mock cron consumer charm – used in scheduler integration tests.

Registers two jobs (``test-job-foo`` and ``test-job-bar``) so the integration
test covers the multi-job case.

Protocol (``cron`` relation):
  - On ``cron-relation-joined``: publish ``jobs`` with both job schedules.
  - On ``cron-relation-changed``: for each ``trigger-<job>`` key in the
    scheduler's databag, compare it against the consumer's stored
    ``ack-<job>`` value.  A mismatch means the job has newly fired; run it
    and respond with ``<job>: done`` plus ``ack-<job>: <same timestamp>``.

The ``ack-<job>`` key lets the consumer identify exactly which jobs fired
without relying on which key triggered the relation-changed event.
"""

from __future__ import annotations

import json
import logging

import ops

logger = logging.getLogger(__name__)

JOBS = {
    "test-job-foo": "* * * * *",
    "test-job-bar": "* * * * *",
}


class MockCronConsumerCharm(ops.CharmBase):
    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.cron_relation_joined, self._on_cron_relation_joined)
        self.framework.observe(self.on.cron_relation_changed, self._on_cron_relation_changed)

    def _on_install(self, _event: ops.InstallEvent) -> None:
        self.unit.status = ops.ActiveStatus("Ready")

    def _on_cron_relation_joined(self, event: ops.RelationJoinedEvent) -> None:
        event.relation.data[self.unit]["jobs"] = json.dumps(JOBS)
        logger.info("Registered jobs %s with scheduler", list(JOBS))
        self.unit.status = ops.ActiveStatus("Waiting for trigger")

    def _on_cron_relation_changed(self, event: ops.RelationChangedEvent) -> None:
        scheduler_units = [u for u in event.relation.units if "scheduler" in u.app.name]
        for sched_unit in scheduler_units:
            sched_data = event.relation.data[sched_unit]
            our_data = event.relation.data[self.unit]

            for key, trigger_ts in sched_data.items():
                if not key.startswith("trigger-"):
                    continue
                job_name = key[len("trigger-"):]
                ack_key = f"ack-{job_name}"

                if our_data.get(ack_key) == trigger_ts:
                    continue  # already processed this trigger

                logger.info("New trigger for job %s – reporting done", job_name)
                our_data[job_name] = "done"
                our_data[ack_key] = trigger_ts

        done_jobs = [j for j in JOBS if event.relation.data[self.unit].get(j) == "done"]
        if len(done_jobs) == len(JOBS):
            self.unit.status = ops.ActiveStatus("all jobs executed successfully")


if __name__ == "__main__":
    ops.main(MockCronConsumerCharm)
