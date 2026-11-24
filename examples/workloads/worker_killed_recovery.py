"""Temps made on two workers, one worker SIGKILLed, readers force recovery tasks."""
import time
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
victim = c.start_worker(cores=2)
c.start_worker(cores=2)
temps, tasks = [], []
for i in range(8):
    o = m.declare_temp()
    t = Task(f"sleep 1 && echo {i} > o")
    t.add_output(o, "o")
    t.set_cores(1)
    m.submit(t)
    temps.append(o)
    tasks.append(t)
c.wait_all(m, tasks)
c.kill_worker(victim)
c.start_worker(cores=2)
time.sleep(1)
r = Task("cat i* > all")
for i, o in enumerate(temps):
    r.add_input(o, f"i{i}")
out = m.declare_file(c.scratch("recovered.txt"))
r.add_output(out, "all")
m.submit(r)
c.wait_all(m, [r])
tasks.append(r)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=3, recovery_min=1)
