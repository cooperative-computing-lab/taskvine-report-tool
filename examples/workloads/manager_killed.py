"""The Manager process is SIGKILLed while tasks run, so the log has no end."""
import os
import signal
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
w = c.start_worker(cores=2)
tasks = []
for i in range(6):
    t = Task("sleep 2" if i < 2 else "sleep 30")
    t.set_cores(1)
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks[:2], timeout=15)
c.expect(tasks=len(tasks), expect_status="PARTIAL", expect_partial_reasons=["manager_end_missing_or_log_truncated"],
         manager_killed=True, workers=1)
c.kill_worker(w)
os.kill(os.getpid(), signal.SIGKILL)
