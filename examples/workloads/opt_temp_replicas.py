"""Temp files replicated to three workers, read downstream, then unlinked while replicas exist."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
m.enable_peer_transfers()
m.tune("temp-replica-count", 3)
for _ in range(3):
    c.start_worker()
tasks, temps = [], []
for i in range(4):
    tmp = m.declare_temp()
    t = Task(f"head -c 200000 /dev/urandom > out")
    t.add_output(tmp, "out")
    m.submit(t)
    tasks.append(t)
    temps.append(tmp)
c.wait_all(m, tasks)
readers = []
for tmp in temps:
    r = Task("wc -c < in")
    r.add_input(tmp, "in")
    m.submit(r)
    readers.append(r)
c.wait_all(m, readers)
for tmp in temps:
    m.remove_file(tmp)
c.pump(m, 3)
tasks += readers
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=3)
