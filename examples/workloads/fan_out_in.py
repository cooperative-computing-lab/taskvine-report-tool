"""One producer, 60 readers of its temp output, one reducer of their 60 temps."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
for _ in range(3):
    c.start_worker(cores=4)
seed = m.declare_temp()
p = Task("head -c 100000 /dev/urandom > seed")
p.add_output(seed, "seed")
m.submit(p)
tasks, outs = [p], []
for i in range(60):
    o = m.declare_temp()
    t = Task(f"wc -c seed > o && echo {i} >> o")
    t.add_input(seed, "seed")
    t.add_output(o, "o")
    m.submit(t)
    tasks.append(t)
    outs.append(o)
r = Task("cat o* > all")
for i, o in enumerate(outs):
    r.add_input(o, f"o{i}")
result = m.declare_file(c.scratch("fan-all.txt"))
r.add_output(result, "all")
m.submit(r)
tasks.append(r)
c.wait_all(m, tasks)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=3)
