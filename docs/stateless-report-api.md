# Stateless report API and concurrency contract

The report server has no process-wide selected log. Every read is addressed by
`run_id` and a content-derived `revision`, and every response repeats that
identity. This prevents one browser tab, user, or request from changing the
data observed by another.

## Request lifecycle

1. `GET /api/v1/runs/<run_id>` verifies the current parser outputs and
   atomically publishes an immutable snapshot if needed.
2. The client pins the returned 64-character SHA-256 revision.
3. Summary, task, chart, and export requests include both run and revision in
   the URL.
4. The server resolves the exact snapshot into Flask request context. Chart
   handlers never read shared mutable run state.
5. The response envelope and `X-TaskVine-Run` / `X-TaskVine-Revision` headers
   repeat the resolved identity. The client checks it before rendering.

Changing parser output creates a new revision. Existing revision URLs continue
to read their old data. `POST /api/v1/runs/<run_id>/refresh` moves only that
run's atomic current pointer; it cannot alter another request's pinned view.

## Snapshot publication

Snapshots live below each run at
`.vine-report/snapshots/<revision>/`. A parse manifest, when present, is the
commit marker: every listed file must match its recorded size and SHA-256, and
parse diagnostics must identify the same source. A mismatch returns HTTP 409
`parse_in_progress`; partial parser output is never published.

Legacy parsed runs without a manifest are still supported by hashing and
copying their CSV files. The copy is verified before an atomic rename. Mutable
SVG render caches live separately under `.vine-report/cache/<revision>/` and
cannot change snapshot identity.

## Concurrency and resource limits

`vine_report` uses a fixed request pool and bounded pending queue instead of
creating one thread per connection or accumulating unlimited queued work. Once
the queue is full, the accept loop applies backpressure. The default is eight
threads and can be adjusted with `--threads` from 1 through 64. Polars and
numerical-library pools default to four threads; set
`VINE_REPORT_COMPUTE_THREADS` before startup to tune that budget.

Example for an HPC login node reached through an SSH local forward:

```bash
vine_report --logs-dir /path/to/logs --host 127.0.0.1 --port 9122 --threads 8
```

## Client isolation

Selecting a run creates a new client generation and aborts the previous run's
requests. Every module also tracks its own request and render sequence. A late
response is discarded unless run, revision, generation, and sequence still
match. A late request cannot remove the newer request's loading state.

## Health and operator diagnostics

`GET /api/v1/health` reports the service/API/parser versions, run count, uptime,
and bounded request counters without hashing or opening run snapshots.
`GET /api/v1/diagnostics` adds per-run current revision, snapshot count and
bytes, last-access timestamps, and parser quality. It does not expose absolute
filesystem paths or raw parser errors.

## Snapshot lifecycle

`vine_report gc` is dry-run-first. The default retains the three most recently
used revisions per run, always retains the current revision, and excludes
revisions accessed within the minimum-age window. `--apply` atomically moves a
candidate out of the snapshot namespace before deleting it and removes only
that revision's SVG cache and access marker.

Every server holds a shared catalog lease. GC requires an exclusive lease and
therefore fails with `gc_server_active` while any process serves the same logs
directory. This trades maintenance availability for deterministic safety.

```bash
vine_report gc --logs-dir /path/to/logs --keep 3 --min-age-hours 24 --dry-run
vine_report gc --logs-dir /path/to/logs --keep 3 --min-age-hours 24 --apply
```

## Acceptance criteria

- legacy global run-switch and lock endpoints return 404;
- cross-run revision use returns 404;
- a source refresh creates a new revision while the old revision remains
  readable;
- parse-manifest mismatch returns 409 without publishing a snapshot;
- mixed parallel requests always return the requested run and revision;
- browser code aborts obsolete requests and rejects late identity mismatches;
- the server remains within its configured request and compute thread budgets.
- health and diagnostics expose no absolute log path;
- GC preserves the current revision and is rejected while a server lease is active.

Run the repeatable real-server identity and latency benchmark with:

```bash
python scripts/benchmark_report.py --base-url http://127.0.0.1:9122 \
  --clients 32 --requests 200
```
