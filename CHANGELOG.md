# Changelog

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
