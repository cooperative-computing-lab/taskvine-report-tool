"""One library with three functions serving 60 FunctionCalls."""
import _common as c
import _library as lib
from ndcctools.taskvine import FunctionCall

m = c.manager()
c.start_worker(cores=4)
library = m.create_library_from_functions("ex-lib", lib.square, lib.concat, lib.slow, add_env=False, exec_mode="fork")
library.set_cores(4)
library.set_function_slots(4)
m.install_library(library)
calls = []
for i in range(60):
    if i % 3 == 0:
        t = FunctionCall("ex-lib", "square", i)
    elif i % 3 == 1:
        t = FunctionCall("ex-lib", "concat", i, "x")
    else:
        t = FunctionCall("ex-lib", "slow", i)
    m.submit(t)
    calls.append(t)
c.wait_all(m, calls)
c.expect(tasks=len(calls), outcomes=c.outcome_counts(calls), workers=1, libraries_min=1, function_calls=len(calls))
