"""Running and never-dispatchable tasks cancelled by id, next to tasks that complete."""
import time
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker(cores=4)
slow = []
for i in range(4):
    t = Task("sleep 30")
    t.set_cores(1)
    m.submit(t)
    slow.append(t)
huge = Task("true")
huge.set_cores(64)
m.submit(huge)
quick = []
for i in range(4):
    t = Task("true")
    t.set_cores(1)
    quick.append(t)
c.pump(m, 15, until=lambda: m.stats.tasks_running >= 4)
for t in slow + [huge]:
    m.cancel_by_task_id(t.id)
for t in quick:
    m.submit(t)
c.wait_all(m, slow + [huge] + quick, timeout=40)
everything = slow + [huge] + quick
c.expect(tasks=len(everything), outcomes=c.outcome_counts(everything), workers=1)
