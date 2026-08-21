# Metric architecture

The report has one public metric registry: `src/vine_report/metric_registry.py`.
It owns metric IDs, user-facing text, units, renderer selection, source semantics,
and bounded-series policy. Do not duplicate these values in routes or browser
modules.

## Data flow

1. `vine_parse` converts source records into long-form fact artifacts and writes
   `metric_contracts.csv` plus metric-specific quality artifacts.
2. `SnapshotStore` publishes those artifacts as an immutable `run_id + revision`.
3. `chart_service` validates typed query parameters, reads only that revision,
   applies time/series filters and extrema-preserving sampling, and returns one
   response envelope.
4. `GenericMetricModule` renders standard line, step, scatter, and point charts.
   Only structurally unique visualizations keep custom modules.

Legacy `/api/*` and `/api/v1/.../charts/*` routes remain compatibility adapters.
New frontend code should use `/api/v1/.../metrics/<metric_id>`.

## Required invariants

- Base storage units are bytes. Unit conversion belongs to display formatting.
- State-series sampling preserves endpoints and global extrema.
- Server-side default series limits must disclose available, selected, omitted,
  and selection-policy metadata. An explicit `series` query is never limited.
- Missing artifacts or unsupported parameters fail closed; they are not replaced
  with invented zeroes.
- Every response is pinned to an immutable revision. Browser generation guards
  discard late responses after a run switch.
- `PASS` means the metric-specific quality policy passed. `PARTIAL` data remains
  visible with its source, coverage, and caveat.

## Cache occupancy semantics

`Worker Cache Occupancy` means TaskVine-managed cached payload, not physical disk
usage. Each `cache-update` starts a residency with an immutable size. An observed
`cache-invalid` or `unlink` ends it; manager-end closure is right-censored and
marked inferred. The aggregate curve is independently reconciled with performance
`inuse_cache` samples. Summary peak and graph peak call the same reducer.

Primary artifacts:

- `cache_residency.parquet`: one row per residency, including evidence.
- `metric_worker_cache_occupancy.csv`: event deltas used by the API.
- `metric_worker_cache_occupancy_quality.csv`: coverage and reconciliation.
- `metric_contracts.csv`: public status and caveats.

## Adding a standard metric

1. Produce a source-backed artifact and quality contract during parsing.
2. Add one `MetricSpec`; prefer an existing generic mode and renderer.
3. Add query-service tests for units, filtering, sampling, and incomplete input.
4. Validate one complete and one incomplete real run.
5. Add a custom route or JavaScript module only when the response geometry cannot
   be expressed by the generic series envelope.
