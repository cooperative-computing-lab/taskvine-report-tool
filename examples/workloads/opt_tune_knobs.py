"""Many vine_tune knobs at unusual values: tiny perf-log interval, keepalives, hungry minimum, retrievals."""
import _common as c
from ndcctools.taskvine import Task

m = c.manager()
for name, value in [("perf-log-interval", 1), ("hungry-minimum", 1), ("hungry-minimum-factor", 1), ("wait-for-workers", 2),
                    ("worker-retrievals", 0), ("max-retrievals", 1), ("transient-error-interval", 1),
                    ("keepalive-interval", 1), ("keepalive-timeout", 30), ("temp-replica-count", 2),
                    ("file-source-max-transfers", 1), ("worker-source-max-transfers", 1), ("immediate-recovery", 1),
                    ("attempt-schedule-depth", 3), ("prefer-dispatch", 1), ("short-timeout", 2),
                    ("resource-management-interval", 1), ("update-interval", 1), ("max-new-workers", 1),
                    ("large-task-check-interval", 1000000), ("clean-redundant-replicas", 1), ("shift-disk-load", 1),
                    ("transfer-replica-per-cycle", 1), ("ramp-down-heuristic", 1), ("monitor-interval", 1)]:
    m.tune(name, value)
c.start_worker()
c.start_worker()
tasks = []
for i in range(12):
    t = Task(f"sleep 0.3; echo {i} > out")
    t.add_output(m.declare_temp(), "out")
    m.submit(t)
    tasks.append(t)
c.wait_all(m, tasks, timeout=60)
c.pump(m, 3)
c.expect(tasks=len(tasks), outcomes=c.outcome_counts(tasks), workers=2)
