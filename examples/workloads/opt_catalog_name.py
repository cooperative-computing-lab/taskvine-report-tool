"""A named Manager that reports to an unreachable catalog, and a worker that names it."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager(name=f"vine-report-example-{c.PORT}")
m.set_catalog_servers(f"127.0.0.1:{c.PORT + 300}")
m.tune("update-interval", 1)
c.start_worker()
tasks = [Task(f"sleep 0.5; echo {i}") for i in range(6)]
for t in tasks:
    m.submit(t)
c.wait_all(m, tasks)
c.pump(m, 3)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
