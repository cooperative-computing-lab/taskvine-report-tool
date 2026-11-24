"""Workers with --single-shot, --idle-timeout, resource overrides, and several per host."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker(cores=3, memory=777, disk=1234, extra=["--single-shot"])
c.start_worker(cores=1, memory=300, disk=500, extra=["--idle-timeout", "3"])
c.start_worker(cores=2, memory=1000, disk=900, extra=["--disk-percent", "50", "--wall-time", "300"])
tasks = []
for i in range(9):
    t = Task("sleep 0.3")
    t.set_cores(1)
    t.set_memory(100)
    t.set_disk(50)
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks, timeout=60)
c.pump(m, 6)  # the idle worker leaves
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=3)
