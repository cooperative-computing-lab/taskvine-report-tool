"""Fast abort of a slow outlier: disconnect_slow_workers with a small multiplier."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
m.enable_disconnect_slow_workers(2)
c.start_worker(cores=4)
c.start_worker(cores=4)
tasks = []
for i in range(16):
    t = Task("sleep 0.3" if i != 12 else "sleep 2")
    t.set_cores(1)
    t.set_retries(3)
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks, timeout=80)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks))
