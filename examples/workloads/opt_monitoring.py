"""Resource monitor with time series, watchdog, and a snapshot file."""
import _common as c
from ndcctools.taskvine import Task, cvine

m = c.manager()
m.enable_monitoring(watchdog=True, time_series=True)
c.start_worker(cores=2)
snap = c.scratch("snap.json")
with open(snap, "w") as h:
    h.write('{"label": "phase", "on-create": "phase-file"}\n')
tasks = []
for i in range(4):
    t = Task("sleep 1; touch phase-file; sleep 1; echo done")
    t.set_cores(1)
    t.set_memory(100)
    t.set_disk(50)
    if i == 0:
        # Task.set_snapshot_file() passes its argument to C as is, which wants a vine_file (both builds).
        cvine.vine_task_set_snapshot_file(t._task, m.declare_file(snap)._file)
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks, timeout=60)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
