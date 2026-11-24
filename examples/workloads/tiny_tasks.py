"""Thousands of tiny tasks on three workers."""
import _common as c
from ndcctools.taskvine import Task

N = 2000
m = c.manager()
for _ in range(3):
    c.start_worker(cores=4)
tasks = []
for i in range(N):
    t = Task(":")
    t.set_cores(1)
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks, timeout=55)
c.expect(tasks=N, outcomes=c.outcome_counts(tasks), workers=3)
