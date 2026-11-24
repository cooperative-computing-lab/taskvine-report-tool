"""Worker features and tasks that require them, one of which no worker has."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker(extra=["--feature", "gpu-like", "--feature", "fast disk"])
c.start_worker(extra=["--feature", "plain"])
tasks = []
for i in range(6):
    t = Task(f"echo {i}")
    t.add_feature(["gpu-like", "plain", "fast disk"][i % 3])
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks)
never = Task("echo never")
never.add_feature("nobody-has-this")
m.submit(never)
c.pump(m, 2)
m.cancel_by_task_id(never.id)
c.wait_all(m, [never], timeout=20)
tasks.append(never)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=2)
