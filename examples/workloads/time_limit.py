"""Tasks that exceed their wall time limit next to tasks that do not."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker(cores=4)
tasks = []
for i in range(3):
    t = Task("sleep 20")
    t.set_time_max(2)
    tasks.append(t)
for i in range(3):
    t = Task("sleep 0.2")
    t.set_time_max(20)
    tasks.append(t)
for t in tasks:
    t.set_cores(1)
    m.submit(t)
c.wait_all(m, tasks, timeout=50)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
