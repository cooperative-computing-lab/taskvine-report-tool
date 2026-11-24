"""vine_graph: Function, Command, and Expand tasks in one graph."""
# producers: vine-graph
import _common as c
from ndcctools.taskvine import vine_graph as vg


def add(a, b):
    return a + b


def build(n):
    return [vg.Function(add, i, n) for i in range(n)]


c.start_worker(cores=4)
with c.executor() as ex:
    a = vg.Function(add, 1, 2)
    b = vg.Function(add, a, 10)
    cmd = vg.Command("echo {x}", x=b)
    e = vg.Expand(build, 5)
    s = vg.Function(sum, e)
    ex.run([cmd, s])
    assert cmd.result().strip() == "13", cmd.result()
    assert s.result() == sum(i + 5 for i in range(5))
c.expect(graph_tasks_min=4, workers=1, libraries_min=1)
