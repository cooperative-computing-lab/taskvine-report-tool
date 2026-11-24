"""Tasks whose commands exit nonzero, and one killed by a signal."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker()
tasks = [Task(f"exit {code}") for code in (0, 1, 3, 42, 255)]
tasks.append(Task("kill -9 $$"))
for t in tasks:
    m.submit(t)
c.wait_all(m, tasks)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), exit_codes=c.exit_codes(tasks), workers=1)
