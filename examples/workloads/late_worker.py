"""Tasks wait several seconds before the first worker joins."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
tasks = []
for i in range(20):
    t = Task(f"echo {i}")
    m.submit(t)
    tasks.append(t)
c.pump(m, 4)
c.start_worker()
c.wait_all(m, tasks)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
