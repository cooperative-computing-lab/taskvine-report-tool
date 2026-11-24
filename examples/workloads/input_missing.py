"""Tasks whose file:// URL input does not exist fail its transfer repeatedly and are cancelled; others run."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker(cores=2)
good = [Task("true") for _ in range(3)]
for t in good:
    t.set_cores(1)
    m.submit(t)
c.wait_all(m, good, timeout=30)
missing = m.declare_url("file://" + c.scratch("no-such-file.txt"))
bad = []
for i in range(2):
    t = Task("cat in")
    t.add_input(missing, "in")
    t.set_cores(1)
    m.submit(t)
    bad.append(t)
# TaskVine retries a failed input transfer after a pause; let it fail at least twice.
c.pump(m, 18)
for t in bad:
    m.cancel_by_task_id(t.id)
c.wait_all(m, bad, timeout=20)
c.expect(tasks=5, outcomes=c.outcome_counts(bad + good), workers=1)
