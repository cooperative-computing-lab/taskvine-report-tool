"""A task whose stdout holds NUL bytes, with a small max-task-stdout-storage.

Upstream master (0bcf6ac65) waits 60 s and then reports "actual received stdout size (-1 bytes)" for this task and
never returns it, so this workload runs on the fork only.
# producers: vine-graph
"""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
m.tune("max-task-stdout-storage", 1000)
c.start_worker()
tasks = [Task("head -c 100 /dev/zero"), Task("printf 'a\\0b\\0c'")]
for t in tasks:
    m.submit(t)
c.wait_all(m, tasks, timeout=40)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
