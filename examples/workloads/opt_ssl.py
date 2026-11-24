"""An SSL Manager with an SSL worker."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager(ssl=True)
c.start_worker(extra=["--ssl"])
tasks = [Task(f"echo {i}") for i in range(5)]
for t in tasks:
    m.submit(t)
c.wait_all(m, tasks)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
