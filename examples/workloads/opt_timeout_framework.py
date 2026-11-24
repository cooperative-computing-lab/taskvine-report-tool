"""The fork's timeout framework: response-timeout with probe links and keepalives at T/2.
# producers: vine-graph
"""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
m.tune("response-timeout", 3)
m.tune("manager-response-timeout", 3)
c.start_worker(extra=["--response-timeout", "3"])
tasks = [Task(f"sleep 0.5; echo {i}") for i in range(6)]
for t in tasks:
    m.submit(t)
c.wait_all(m, tasks)
c.pump(m, 4)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
