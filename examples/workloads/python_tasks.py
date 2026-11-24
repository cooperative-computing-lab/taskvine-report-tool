"""PythonTasks with arguments and results."""
import _common as c
import _library as lib
from ndcctools.taskvine import PythonTask

m = c.manager()
c.start_worker(cores=2)
tasks = [PythonTask(lib.concat, i, "p") for i in range(8)]
for t in tasks:
    t.set_cores(1)
    m.submit(t)
c.wait_all(m, tasks, timeout=55)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
