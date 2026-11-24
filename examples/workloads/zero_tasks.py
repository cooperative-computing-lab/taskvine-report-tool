"""A Manager that a worker joins but that never runs a task."""
import _common as c

m = c.manager()
c.start_worker()
c.pump(m, 10, until=lambda: m.stats.workers_connected >= 1)
c.pump(m, 2)
c.expect(tasks=0, outcomes={}, workers=1)
