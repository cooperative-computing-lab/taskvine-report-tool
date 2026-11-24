"""Shared body for peer_on and peer_off: outputs made on several workers read across workers."""
import _common as c
from ndcctools.taskvine import Task


def run(peer):
    m = c.manager()
    if peer:
        m.enable_peer_transfers()
    else:
        m.disable_peer_transfers()
    for _ in range(3):
        c.start_worker(cores=2)
    temps, tasks = [], []
    for i in range(12):
        # Without peer transfers a temp cannot move, so outputs come back to the Manager as files.
        o = m.declare_temp() if peer else m.declare_file(c.scratch(f"peer-{i}.bin"))
        t = Task(f"head -c 3000000 /dev/zero > o && sleep 0.5")
        t.add_output(o, "o")
        m.submit(t)
        temps.append(o)
        tasks.append(t)
    c.wait_all(m, tasks)
    readers = []
    for i in range(12):
        t = Task("cat a b c > /dev/null")
        for name, o in zip("abc", (temps[i], temps[(i + 4) % 12], temps[(i + 8) % 12])):
            t.add_input(o, name)
        m.submit(t)
        readers.append(t)
    c.wait_all(m, readers)
    tasks += readers
    c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=3, peer_transfers=peer)
