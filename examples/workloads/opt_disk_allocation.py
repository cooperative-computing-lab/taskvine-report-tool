"""Disk allocations per task and a reduced disk proportion available to tasks."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
m.tune("disk-proportion-available-to-task", 0.5)
c.start_worker(disk=2000)
tasks = []
for i in range(5):
    path = c.scratch("shared", f"in-{i}")
    open(path, "w").write("z" * 100000)
    t = Task("wc -c < in")
    t.add_input(m.declare_file(path), "in")
    t.set_disk(10 + i * 100)
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
