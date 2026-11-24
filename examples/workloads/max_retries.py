"""A task whose worker is killed under it repeatedly.

The vine-graph fork charges each loss as a try, so the task ends MAX_RETRIES; upstream counts only completed tries,
so it is dispatched again until a worker lets it finish.  The outcome is read from the API either way.
"""
import time
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
t = Task("sleep 8")
t.set_retries(1)
t.set_cores(1)
ok = Task("true")
ok.set_cores(1)
m.submit(ok)
m.submit(t)
returned = c._returned


def pump(seconds):
    c.pump(m, seconds)


losses = 0
while t.id not in returned and losses < 4:
    w = c.start_worker(cores=2)
    pump(3)
    if t.id in returned:
        break
    c.kill_worker(w)
    losses += 1
    # Space losses past the five second window in which one loss may excuse another.
    pump(6)
if t.id not in returned or ok.id not in returned:
    c.start_worker(cores=2)
    c.wait_all(m, [x for x in (t, ok) if x.id not in returned], timeout=40)
c.expect(tasks=2, outcomes=c.outcome_counts([t, ok]), worker_loss_attempts_min=1)
