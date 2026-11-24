"""Resource categories in each allocation mode, with an exhaustion that retries at the maximum."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
m.enable_monitoring()
m.tune("category-steady-n-tasks", 2)
c.start_worker(cores=4, memory=4000)
modes = {"cat-fixed": "fixed", "cat-max": "max", "cat-waste": "min waste", "cat-throughput": "max throughput"}
for cat, mode in modes.items():
    m.set_category_mode(cat, mode)
    m.set_category_resources_max(cat, {"cores": 2, "memory": 400, "disk": 200})
    m.set_category_first_allocation_guess(cat, {"cores": 1, "memory": 100, "disk": 50})
# set_category_max_concurrent() is not used: it refers to self._work_queue, which Manager lacks (both builds).
tasks = []
for cat in modes:
    for i in range(4):
        t = Task("sleep 0.2; echo ok")
        t.set_category(cat)
        m.submit(t)
        tasks.append(t)
# Allocates 150 MB against a first guess of 100 MB: exhausted, then retried at the category maximum.
big = Task("python3 -c 'b = bytearray(150*1024*1024); import time; time.sleep(2)'")
big.set_category("cat-max")
m.submit(big)
tasks.append(big)
c.wait_all(m, tasks, timeout=80)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
