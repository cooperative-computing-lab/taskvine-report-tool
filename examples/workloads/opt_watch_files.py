"""Watched output files streamed back while the task runs."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker()
tasks = []
for i in range(3):
    out = m.declare_file(c.scratch("watch", f"stream-{i}.txt"))
    t = Task("for k in 1 2 3 4 5 6; do echo line $k >> stream; sleep 0.5; done")
    t.add_output(out, "stream", watch=True)
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
