"""A small max-task-stdout-storage with huge, binary, and empty outputs."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
m.tune("max-task-stdout-storage", 1000)
c.start_worker()
commands = ["head -c 5000000 /dev/urandom", "printf ''", "seq 1 200000", "printf 'no newline at end'"]
tasks = []
for command in commands:
    t = Task(command)
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks, timeout=60)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
