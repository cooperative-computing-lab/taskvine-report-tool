"""One standard task on one worker."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker()
t = Task("echo hello")
m.submit(t)
c.wait_all(m, [t])
c.expect(tasks=1, task_ids=[t.id], outcomes=c.outcome_counts([t]), exit_codes=c.exit_codes([t]), workers=1)
