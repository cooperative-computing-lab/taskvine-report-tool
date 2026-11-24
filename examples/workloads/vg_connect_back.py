"""vine_graph: checkpoints received from workers that connect back to the data port."""
# producers: vine-graph
import _common as c
from ndcctools.taskvine import vine_graph as vg


def make(i):
    return bytes([i]) * 2_000_000


def total(*parts):
    return sum(len(p) for p in parts)


c.start_worker(cores=2, extra=("--connect-back",))
with c.executor() as ex:
    parts = [vg.Function(make, i) for i in range(4)]
    t = vg.Function(total, *parts)
    ex.run(t)
    assert t.result() == 8_000_000
c.expect(graph_tasks_min=5, workers=1, checkpoints_min=1)
