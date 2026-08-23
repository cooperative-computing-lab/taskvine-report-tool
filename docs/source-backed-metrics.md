# Source-backed metric contract

TaskVine Report metrics are projections of immutable source evidence, not
best-effort interpretations created during an HTTP request. `vine_parse`
produces the metric CSV files and `metric_contracts.csv` before committing the
parse manifest. `vine_report` snapshots those files with the existing
content-addressed revision.

## Source ownership

| Source | Facts owned by the report tool |
| --- | --- |
| `transactions` | task state observations, ordered execution tries, worker sessions, transfer payloads |
| `performance` | manager capacity and committed-resource samples, aggregate conservation counters |
| debug-derived task objects | application, recovery, and library classification; compatibility evidence |

The upstream files are unversioned. Their local adapters and every derived
metric therefore have explicit semantic versions. A source format change must
fail closed or lower the affected metric contract to `PARTIAL`; it must never
silently reinterpret an old field.

## Metric semantics

| Metric | Unit | Inclusion | Explicit exclusion |
| --- | --- | --- | --- |
| `task-throughput` | attempts/s | terminal application attempts | library, recovery, incomplete attempts |
| `workflow-pressure` | attempts | observed queued, executing, and retrieving states | library and recovery attempts |
| `retry-pressure` | attempts/s | application READY observations by application-level try | library and recovery attempts |
| `task-latency` | seconds | application attempts with phase endpoints | missing phase endpoints |
| `failure-timeline` | attempts | unsuccessful application attempts | library, recovery, incomplete attempts |
| `worker-capacity` | cores/count | manager performance samples | occupancy at zero capacity |
| `worker-churn` | events/s | observed worker joins and disconnect reasons | synthetic disconnects |
| `core-occupancy` | ratio | committed cores divided by total cores | zero-capacity samples |
| `transfer-throughput` | bytes/s | completed input/output payloads | avoided transfers and cache hits |

`committed_cores / total_cores` is scheduler occupancy, not CPU utilization.
Transfer throughput is payload throughput, not physical network utilization.
The transaction transfer-size token is normalized as bytes because its sums
are reconciled against the performance byte counters.

## Quality gate

A metric is plot-ready only when its contract status is `PASS`. The API returns
the contract but no series for `PARTIAL` or `UNAVAILABLE` metrics.

The current gates require:

1. no malformed required records;
2. at least 99.5% terminal and task-classification coverage for terminal task metrics;
3. a real manager end for terminal task and transfer metrics;
4. at least 99.5% usable performance samples for worker capacity;
5. transfer input/output byte sums within 1% of performance counters;
6. exact worker-connection conservation when the performance total is present;
7. aggregation before presentation downsampling, preserving endpoints and extrema.

Synthetic debug end times are never promoted to source terminal events. An
incomplete run may still expose independently valid observed-state, retry,
worker-event, and capacity metrics. Its application completion and transfer
charts remain unavailable, and open workflow states are explicitly
right-censored at the last source observation.

## API

```
GET /api/v1/runs/<run_id>/revisions/<revision>/metrics/<metric_id>
```

Every response is pinned to the requested run and revision. The response
contains `available`, `contract`, axis labels, chart kind, and named series.
Unknown metrics return `metric_not_found`.

The browser polls `GET /api/v1/runs/<run_id>/watch`, a bounded source-token
check, while the page is visible. It requests `POST .../refresh` only after the
committed parser source changes; refresh remains the sole owner of full output
verification and immutable revision publication.

## Release acceptance

- Unit fixtures prove retry identity, source conservation, and incomplete-run
  rejection.
- API tests prove revision identity and absence of plot points for failed
  contracts.
- A complete real run must pass all advertised source metric contracts.
- A truncated real run must hide terminal task and transfer series while
  preserving independently valid live metrics.
- Existing request isolation, concurrency, packaging, and browser tests remain
  green.
