"""vine_graph: a long chain whose intermediate results are released and one asked for again."""
# producers: vine-graph
import _common as c
from ndcctools.taskvine import vine_graph as vg


def inc(x):
    return x + 1


c.start_worker(cores=2)
with c.executor() as ex:
    first = vg.Function(inc, 0)
    t = first
    for _ in range(30):
        t = vg.Function(inc, t)
    ex.run(t)
    assert t.result() == 31
    assert first.result() == 1
c.expect(graph_tasks_min=31, workers=1)
