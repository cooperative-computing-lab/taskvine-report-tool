"""vine_graph: small (inline) and large outputs checkpointed to the vault."""
# producers: vine-graph
import _common as c
from ndcctools.taskvine import vine_graph as vg


def make(i, size):
    return bytes([i % 256]) * size


def total(*parts):
    return sum(len(p) for p in parts)


c.start_worker(cores=2)
c.start_worker(cores=2)
with c.executor() as ex:
    small = [vg.Function(make, i, 100) for i in range(10)]
    large = [vg.Function(make, i, 3_000_000) for i in range(4)]
    t = vg.Function(total, *small, *large)
    ex.run(t)
    assert t.result() == 10 * 100 + 4 * 3_000_000
c.expect(graph_tasks_min=15, workers=2, checkpoints_min=1, inline_checkpoints_min=1)
