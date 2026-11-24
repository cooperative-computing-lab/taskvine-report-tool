"""Files whose local and sandbox names hold spaces, unicode, percent signs, and quotes."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker()
tasks = []
for i, name in enumerate(["with space.txt", "ünïcødé.txt", "per%20cent.txt", "it's.txt", "-dash.txt"]):
    local = c.scratch("odd", f"{i} {name}")
    with open(local, "w") as h:
        h.write(name * 100)
    f = m.declare_file(local)
    out = m.declare_file(c.scratch("odd", f"out {i} {name}"))
    t = Task(f"cat ./'{name}' > out" if "'" not in name else 'cat ./"it'"'"'s.txt" > out')
    t.add_input(f, name)
    t.add_output(out, "out")
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
