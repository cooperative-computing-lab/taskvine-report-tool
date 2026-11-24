"""Files produced by mini tasks and an untar, shared by several tasks."""
import os
import tarfile
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
c.start_worker()
src = c.scratch("tar", "data", "a.txt")
open(src, "w").write("payload\n")
tarball = c.scratch("tar", "data.tar.gz")
with tarfile.open(tarball, "w:gz") as tar:
    tar.add(os.path.dirname(src), arcname="data")
untarred = m.declare_untar(m.declare_file(tarball), cache=True)
mini = Task("seq 1 1000 > made")
made = m.declare_minitask(mini, "made", cache=True)
tasks = []
for i in range(4):
    t = Task("cat d/data/a.txt m | wc -l")
    t.add_input(untarred, "d")
    t.add_input(made, "m")
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=1)
