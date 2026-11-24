# Changelog

## 2026.8.2 - 2026-08-22

### Added

- Plot-wide line inspection that reports every visible series at the cursor's
  x coordinate, with active-first ordering and explicit active/total counts.
- Click-to-pin and keyboard-accessible line cursors; `Escape` releases a pinned
  cursor.
- Shared temporal policies for state and bucket series, plus scale regression
  coverage for 120 series with 240,000 source points.

### Changed

- All time-linked line metrics use the manager runtime as their canonical
  domain. Debug-origin data is aligned once at the service boundary.
- State series explicitly cover both runtime boundaries, while rate and count
  series materialize empty buckets as zero.
- Step paths use SVG clipping so viewport boundaries preserve step geometry.
- Generic scatter metrics can enter and leave Display CDF mode using the same
  shared cursor interaction.

### Compatibility

- Sparse artifacts from older parser versions are normalized by the API.
- Reparse with 2026.8.2 writes dense bucket facts and explicit lifecycle
  boundaries where those are part of the artifact contract.

## 2026.8.1 - 2026-08-20

### Added

- Source-backed task throughput, latency, failure, worker capacity, worker
  churn, and transfer throughput metrics.
- Per-metric semantic versions, coverage, source lineage, exclusions, units,
  and fail-closed quality contracts.
- Cross-source conservation checks between transaction transfers, performance
  byte counters, and worker connection totals.

### Changed

- Application summaries now exclude library and recovery work when validated
  metric facts are available.
- Incomplete new-format runs hide untrusted application KPIs and chart points
  instead of falling back to misleading legacy attempt counts.
- Transaction resource-attempt numbers are no longer treated as execution try
  identifiers; execution tries follow ordered READY events.
- The ambiguous legacy task-concurrency view is hidden pending a versioned
  lifecycle contract.
- The frontend verifies backend capabilities before loading charts and shows a
  single restart instruction when a live server is older than its JS bundle.

### Compatibility

- Older parsed runs remain readable but show source-backed metrics as
  unavailable until reparsed.
- TaskVine core remains unchanged.

## 2026.8.0 - 2026-08-20

### Added

- Content-addressed immutable report revisions and request-local run identity.
- Versioned `/api/v1` summary, task, chart, export, comparison, health, and
  diagnostics endpoints.
- Fail-closed parser diagnostics and committed output manifests.
- Bounded request and compute thread budgets for shared HPC login nodes.
- Safe dry-run-first snapshot garbage collection with cross-process server
  leases.
- Playwright acceptance coverage for independent pages, stale requests,
  reload state, chart geometry, orange hover, and vertical resizing.
- Python 3.10-3.13, distribution, dependency-audit, and browser CI gates.

### Changed

- The browser now pins every read to `run_id + revision` and aborts obsolete
  requests before rendering.
- Mutable SVG caches are stored outside immutable snapshot directories.
- Supported Python versions now accurately begin at Python 3.10.

### Removed

- Process-wide current-run switching, browser locking endpoints, and the
  frontend `LogManager` singleton.

### Compatibility

- Existing parsed CSV directories remain readable. Runs created by older
  parsers surface `UNKNOWN` data quality until reparsed with diagnostics and a
  parse manifest.
- TaskVine core is unchanged.
