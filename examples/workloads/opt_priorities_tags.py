"""Priorities, tags, schedulers, and per-task retries."""
import _common as c
from ndcctools.taskvine import Task, cvine

m = c.manager()
m.set_scheduler("files")
c.start_worker(cores=1)
tasks = []
for i in range(12):
    t = Task(f"echo {i}")
    t.set_priority((i * 7) % 5 - 2.5)
    t.set_tag(f"tag with space #{i} ünï")
    # Task.set_scheduler() asks get_c_constant() for "vine_schedule_<name>", which it prefixes with "VINE_" again,
    # so no name works (both builds); the C call is used instead.
    cvine.vine_task_set_scheduler(t._task, getattr(cvine, "VINE_SCHEDULE_" + ["FILES", "TIME", "RAND", "WORST", "DISK"][i % 5]))
    t.set_retries(i % 3 + 1)
    t.set_cores(1)
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
