"""A call kills its library process; the Manager starts another and the other calls finish."""
import os
import _common as c
import _library as lib
from ndcctools.taskvine import FunctionCall

marker = c.scratch("killed-library")
m = c.manager()
c.start_worker(cores=2)
library = m.create_library_from_functions("ex-killer", lib.kill_library_once, add_env=False, exec_mode="fork")
library.set_cores(2)
library.set_function_slots(2)
m.install_library(library)
calls = []
for i in range(10):
    t = FunctionCall("ex-killer", "kill_library_once", marker, i)
    t.set_retries(5)
    m.submit(t)
    calls.append(t)
c.wait_all(m, calls, timeout=55)
# Upstream drops the worker when its library dies mid-call (the library's output reaches the manager link), and the
# worker reconnects, so the number of connections is not asserted here; the runner still reconciles it.
c.expect(tasks=len(calls), outcomes=c.outcome_counts(calls), libraries_min=2, function_calls=len(calls))
