"""A password-protected Manager: one worker with the right password, one with a wrong one."""
import _common as c
from ndcctools.taskvine import Task

good, bad = c.scratch("pw-good"), c.scratch("pw-bad")
open(good, "w").write("s3cret\n")
open(bad, "w").write("wrong\n")
m = c.manager()
m.set_password_file(good)
c.start_worker(extra=["--password", bad])
c.start_worker(extra=["--password", good])
tasks = [Task(f"echo {i}") for i in range(5)]
for t in tasks:
    m.submit(t)
c.wait_all(m, tasks)
# The rejected worker never becomes a WORKER CONNECTION of the transactions log.
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks))
