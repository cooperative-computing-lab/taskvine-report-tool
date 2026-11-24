"""Hosts blocklisted and unblocked again, and a host drained and undrained."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker()
first = [Task("echo first")]
m.submit(first[0])
c.wait_all(m, first)
# block_host_with_timeout() is not used: its SWIG wrapper rejects a Python int for time_t (both builds).
m.block_host("localhost")
m.block_host("127.0.0.1")
c.pump(m, 2)
m.unblock_host()
c.start_worker()
m.set_draining_by_hostname("localhost", True)
c.pump(m, 1)
m.set_draining_by_hostname("localhost", False)
later = [Task(f"echo {i}") for i in range(4)]
for t in later:
    m.submit(t)
c.wait_all(m, later, timeout=60)
tasks = first + later
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks))
