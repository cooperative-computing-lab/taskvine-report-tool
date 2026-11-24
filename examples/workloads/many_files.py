"""One task with 80 buffer inputs and 80 outputs, and a reader of all its outputs."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker()
t = Task("for i in $(seq 0 79); do cat in$i > out$i; done")
for i in range(80):
    t.add_input(m.declare_buffer(f"buffer {i}\n" * (i + 1)), f"in{i}")
outs = []
for i in range(80):
    o = m.declare_temp()
    t.add_output(o, f"out{i}")
    outs.append(o)
m.submit(t)
r = Task("cat out* | wc -l > count")
for i, o in enumerate(outs):
    r.add_input(o, f"out{i}")
r.add_output(m.declare_file(c.scratch("count.txt")), "count")
m.submit(r)
c.wait_all(m, [t, r])
c.expect(tasks=2, outcomes=c.outcome_counts([t, r]), workers=1, inputs_min={str(t.id): 80, str(r.id): 80},
         outputs_min={str(t.id): 80})
