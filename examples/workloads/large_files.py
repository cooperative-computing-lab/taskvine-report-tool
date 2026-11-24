"""A 40 MB declared input read by four tasks that each write a 20 MB output file."""
import os
import _common as c
from ndcctools.taskvine import Task

src = c.scratch("big-input.bin")
with open(src, "wb") as h:
    h.write(os.urandom(40 * 2**20))
m = c.manager()
c.start_worker()
c.start_worker()
big = m.declare_file(src, cache=True)
tasks = []
for i in range(4):
    out = m.declare_file(c.scratch(f"big-out-{i}.bin"))
    t = Task("head -c 20971520 big > out")
    t.add_input(big, "big")
    t.add_output(out, "out")
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks)
c.expect(tasks=4, outcomes=c.outcome_counts(tasks), workers=2,
         manager_put_bytes_min=40 * 2**20, cache_update_bytes_min=40 * 2**20 + 4 * 20 * 2**20)
