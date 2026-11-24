"""vine_graph: a layered graph during which one worker is SIGKILLed."""
# producers: vine-graph
import threading
import time
import _common as c
from ndcctools.taskvine import vine_graph as vg


def step(i, *parents):
    import time
    time.sleep(0.3)
    return bytes([i % 256]) * (200_000 if i % 3 == 0 else 2000)


victim = c.start_worker(cores=3)
c.start_worker(cores=3)
with c.executor() as ex:
    prev, every = [], []
    for layer in range(8):
        cur = [vg.Function(step, layer * 10 + j, *prev[j:j + 2]) for j in range(10)]
        every += cur
        prev = cur
    threading.Timer(4, c.kill_worker, args=(victim,)).start()
    ex.run(prev)
c.expect(graph_tasks_min=80, workers=2, worker_loss_attempts_min=1)
