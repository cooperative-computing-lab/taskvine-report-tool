"""file:// URL inputs and buffers shared by several tasks."""
import _common as c
from ndcctools.taskvine import Task

src = c.scratch("url-source.txt")
with open(src, "w") as h:
    h.write("payload\n" * 100000)
m = c.manager()
c.start_worker()
c.start_worker()
url = m.declare_url("file://" + src, cache=True)
buf = m.declare_buffer("shared buffer\n", cache=True)
tasks = []
for i in range(10):
    t = Task("cat u b | wc -l")
    t.add_input(url, "u")
    t.add_input(buf, "b")
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=2)
