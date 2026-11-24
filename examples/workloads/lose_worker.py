"""Simulated loss with Manager.lose_worker(): the worker stays connected and forgets its state."""
# producers: vine-graph
import time
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker(cores=2)
c.start_worker(cores=2)
temps, tasks = [], []
for i in range(6):
    o = m.declare_temp()
    t = Task(f"echo {i} > o")
    t.add_output(o, "o")
    m.submit(t)
    temps.append(o)
    tasks.append(t)
c.wait_all(m, tasks)
victim = m.status("workers")[0]["workerid"]
assert m.lose_worker(victim)
c.pump(m, 3)
r = Task("cat i* > all")
for i, o in enumerate(temps):
    r.add_input(o, f"i{i}")
r.add_output(m.declare_file(c.scratch("after-loss.txt")), "all")
m.submit(r)
c.wait_all(m, [r])
tasks.append(r)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=2, simulated_losses=1)
