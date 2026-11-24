"""A chain of 40 tasks, each reading the temp output of the previous one."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker()
c.start_worker()
prev, tasks = None, []
for i in range(40):
    out = m.declare_temp()
    if prev is None:
        t = Task(f"echo {i} > out")
    else:
        t = Task(f"cat in > out && echo {i} >> out")
        t.add_input(prev, "in")
    t.add_output(out, "out")
    m.submit(t)
    tasks.append(t)
    prev = out
final = m.declare_file(c.scratch("chain-final.txt"))
t = Task("cat in > final")
t.add_input(prev, "in")
t.add_output(final, "final")
m.submit(t)
tasks.append(t)
c.wait_all(m, tasks)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=2)
