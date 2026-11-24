"""Two libraries on two workers, calls to both."""
import _common as c
import _library as lib
from ndcctools.taskvine import FunctionCall

m = c.manager()
c.start_worker(cores=4)
c.start_worker(cores=4)
for name, fn in (("ex-a", lib.square), ("ex-b", lib.concat)):
    library = m.create_library_from_functions(name, fn, add_env=False, exec_mode="fork")
    library.set_cores(2)
    library.set_function_slots(2)
    m.install_library(library)
calls = []
for i in range(30):
    t = FunctionCall("ex-a", "square", i) if i % 2 else FunctionCall("ex-b", "concat", i, i)
    m.submit(t)
    calls.append(t)
c.wait_all(m, calls)
c.expect(tasks=len(calls), outcomes=c.outcome_counts(calls), workers=2, libraries_min=2, function_calls=len(calls))
