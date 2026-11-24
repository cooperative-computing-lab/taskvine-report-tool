"""A direct-mode library with one slot, removed, then a fork-mode library with library log watching."""
import _common as c
import _library as lib
from ndcctools.taskvine import FunctionCall

m = c.manager()
m.tune("watch-library-logfiles", 1)
c.start_worker(cores=4)
direct = m.create_library_from_functions("ex-direct", lib.square, add_env=False, exec_mode="direct")
direct.set_cores(1)
direct.set_function_slots(1)
m.install_library(direct)
first = [FunctionCall("ex-direct", "square", i) for i in range(10)]
for t in first:
    m.submit(t)
c.wait_all(m, first, timeout=40)
m.remove_library("ex-direct")
fork = m.create_library_from_functions("ex-forkmany", lib.concat, add_env=False, exec_mode="fork")
fork.set_cores(2)
fork.set_function_slots(2)
m.install_library(fork)
second = [FunctionCall("ex-forkmany", "concat", i, "\u00fc\nx") for i in range(10)]
for t in second:
    m.submit(t)
c.wait_all(m, second, timeout=40)
calls = first + second
c.expect(tasks=len(calls), outcomes=c.outcome_counts(calls), workers=1, libraries_min=2, function_calls=len(calls))
