"""Tasks that request different cores, memory, disk, and gpus=0."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker(cores=4, memory=4000, disk=8000)
tasks, cores = [], {}
for i, (k, mem, disk) in enumerate([(1, 100, 100), (2, 500, 200), (3, 1000, 300), (4, 2000, 400), (1, 0, 0), (2, 250, 50)]):
    t = Task("sleep 0.3")
    t.set_cores(k)
    if mem:
        t.set_memory(mem)
    if disk:
        t.set_disk(disk)
    t.set_gpus(0)
    m.submit(t)
    tasks.append(t)
    cores[str(t.id)] = k
c.wait_all(m, tasks)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1, cores=cores)
