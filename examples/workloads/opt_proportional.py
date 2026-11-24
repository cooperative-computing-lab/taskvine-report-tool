"""Proportional resources forced on whole tasks, with tasks that request nothing and a large task."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
m.tune("force-proportional-resources", 1)
m.tune("force-proportional-resources-whole-tasks", 1)
c.start_worker(cores=4, memory=4000, disk=4000)
tasks = []
for i in range(6):
    t = Task("sleep 0.2")
    if i % 2:
        t.set_memory(1500)
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
