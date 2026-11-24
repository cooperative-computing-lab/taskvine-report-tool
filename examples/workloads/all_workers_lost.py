"""Every worker is SIGKILLed while tasks run; a new worker joins and finishes them."""
import time
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
first = [c.start_worker(cores=2), c.start_worker(cores=2)]
tasks = []
for i in range(8):
    t = Task("sleep 4")
    t.set_cores(1)
    m.submit(t)
    tasks.append(t)
c.pump(m, 20, until=lambda: m.stats.tasks_running >= 4)
time.sleep(1)
for w in first:
    c.kill_worker(w)
c.pump(m, 4)
c.start_worker(cores=4)
c.wait_all(m, tasks, timeout=50)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=3, worker_loss_attempts_min=4)
