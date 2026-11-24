"""vine_graph: inputs served from the Manager's vault as manager:// transfers.

A 400 KB argument is edata, a VINE_FILE in the vault that dispatch sends as manager://.  Outputs are checkpointed,
then the only worker holding them is SIGKILLed, so readers on a new worker get the checkpoints from the vault too.
"""
# producers: vine-graph
import time
import _common as c
from ndcctools.taskvine import vine_graph as vg


def size(blob):
    return len(blob)


def make(i):
    return bytes([i]) * 300_000


def total(*parts):
    return sum(len(p) for p in parts)


first = c.start_worker(cores=2)
with c.executor() as ex:
    blob = b"x" * 400_000
    sized = [vg.Function(size, blob) for _ in range(2)]
    parts = [vg.Function(make, i) for i in range(4)]
    ex.run(sized + parts)
    # Give the checkpoints of the parts time to reach the vault before their only holder dies.
    time.sleep(3)
    c.kill_worker(first)
    c.start_worker(cores=2)
    t = vg.Function(total, *parts)
    ex.run(t)
    assert t.result() == 4 * 300_000 and sized[0].result() == 400_000
c.expect(graph_tasks_min=7, workers=2, checkpoints_min=4, manager_vault_transfers_min=1)
