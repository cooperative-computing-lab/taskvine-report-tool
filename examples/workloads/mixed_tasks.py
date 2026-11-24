"""Standard tasks, PythonTasks, and FunctionCalls in one run."""
import _common as c
import _library as lib
from ndcctools.taskvine import FunctionCall, PythonTask, Task

m = c.manager()
c.start_worker(cores=4)
c.start_worker(cores=4)
library = m.create_library_from_functions("ex-mixed", lib.square, add_env=False, exec_mode="fork")
library.set_cores(2)
library.set_function_slots(2)
m.install_library(library)
tasks = []
for i in range(10):
    tasks.append(Task(f"echo {i}"))
    tasks.append(PythonTask(lib.square, i))
    tasks.append(FunctionCall("ex-mixed", "square", i))
for t in tasks:
    if not isinstance(t, FunctionCall):
        t.set_cores(1)
    m.submit(t)
c.wait_all(m, tasks, timeout=55)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=2, libraries_min=1, function_calls=10)
