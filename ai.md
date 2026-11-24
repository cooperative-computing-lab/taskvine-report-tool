# AI handoff

Last verified: 2026-08-23 (America/New_York)

This is the current handoff for agents working in
`/users/jzhou24/taskvine-report-tool`. Recheck live state before relying on
process IDs, quota, or benchmark timings.

## Current release state

- Branch: `main`
- Latest functional commit: `808ff285373c9fc6a79fd84e46b69fe48169e70b`
- Version: `2026.8.2`
- Worktree was clean before this handoff document was added.
- The report service is running on `127.0.0.1:9122` with eight request threads.
- Health is `ok`, API schema is `report-api/v1`, parser contract is version 2,
  and the live server reported zero server errors at verification time.

Start or restart the service with:

```bash
/users/jzhou24/miniconda3/envs/dagvine-env/bin/vine_report \
  --host 127.0.0.1 \
  --port 9122 \
  --logs-dir /users/jzhou24/taskvine-report-tool/logs \
  --threads 8
```

Verify it with:

```bash
curl -fsS http://127.0.0.1:9122/api/v1/health
```

## Latest legend requirement and acceptance

The user wants legends to look like a spaced table, not a dense tag stream:

- never show a horizontal scrollbar;
- use responsive, aligned grid columns;
- retain a vertical scrollbar when the legend exceeds its available height;
- keep enough row and column spacing for readability.

Commit `808ff28` implements this with responsive CSS Grid columns. On the real
`lpc_logs` worker-executing-tasks chart, the 24-item legend was verified at a
1200 px viewport with these computed values:

- 4 columns;
- 6 px row gap and 24 px column gap;
- 0 px horizontal overflow;
- 102 px vertical overflow;
- `overflow-y: auto`.

The legend stylesheet cache key is `legend.css?v=2.2`. The regression test is
`legends use a spaced responsive grid without horizontal scrolling` in
`tests/e2e/dashboard.spec.js`.

## Regression status

Latest complete run after the grid change:

- Python unit/integration tests: 76/76 PASS.
- Playwright browser tests: 17/17 PASS.
- Targeted legend Playwright test: 1/1 PASS.
- Real `lpc_logs` layout check: PASS.
- `git diff --check`: PASS.

Use `/tmp` for Playwright output because the user quota is close to full:

```bash
python -m unittest discover -s tests -p 'test_*.py'
npx playwright test --output=/tmp/vine-report-playwright-results
```

## Product behavior that must remain intact

- Line charts inspect all visible series at the cursor x coordinate, including
  correct behavior for series that begin or end partway through a run.
- Line, point, and bar details can be pinned. Clicking elsewhere or pressing
  Escape releases the pinned detail.
- Display CDF must enter and leave cleanly and restore the original points.
- Chart zoom controls remain fixed outside the moving plot viewport and support
  up to 2000% zoom.
- Entity Explorer tables sort by clicked column headers. Task ID ordering uses
  try ID as the secondary key.
- Source-backed metric quality is fail-closed. Do not turn PARTIAL or
  UNAVAILABLE evidence into apparently complete values.
- Requests remain pinned to immutable `run_id + revision`; do not reintroduce
  process-global run selection.

## Relevant code map

- Parser orchestration: `src/vine_parse/data_parser.py`
- Native debug classifier: `src/vine_parse/native/debug_native.c`
- Metric contracts and queries: `src/vine_report/metric_registry.py`,
  `src/vine_report/chart_service.py`, `src/vine_report/temporal_series.py`
- Shared chart implementation: `src/vine_report/static/js/modules/base.js`
- Shared line cursor: `src/vine_report/static/js/modules/line_cursor.js`
- Shared tooltip pinning: `src/vine_report/static/js/modules/tooltip_pin.js`
- Legend layout: `src/vine_report/static/css/legend.css`
- Card scrolling and chart layout: `src/vine_report/static/css/app.css`,
  `src/vine_report/static/css/section.css`
- Entity explorer: search `src/vine_report/static/js/main.js` and API routes for
  `explorer`.

Real validation data:

- `logs/lpc_logs`
- `logs/trijet_1000to1500_71510`

## Operational warning

The `/users/jzhou24` quota reached 100% during the latest change and caused an
`apply_patch` write to truncate three files. They were fully restored from Git,
the intended patch was reapplied, and all tests passed afterward. Temporary
relocation was used only to recover space. `node_modules`, `build`, `dist`, the
project `tmp` directory, and both runs' PNG/HTML exports have all been restored
to their original project paths. Raw logs and CSV data were never moved or
deleted.

Before builds or browser tests, run:

```bash
quota /users/jzhou24
git status --short
```

If quota is exhausted again, write test output to `/tmp` and do not modify
source until a small write-and-restore check succeeds. Do not delete or relocate
raw logs, parsed CSVs, or user data merely to make room.

## Producer compatibility

Both upstream cctools master and the vine-graph fork are supported. The fork's
joined task descriptions are expanded in `sources/debug_legacy.py`; fork message
families (checkpoint, simulated_loss, task_lost_worker, fork_info, manager://)
are in `protocol.py` (RULESET_VERSION 2, mirrored in `native/debug_native.c`).
Final results come from transactions RETRIEVED records, and
`reconcile_with_transactions()` makes any lost dispatch, attribution, cores,
DONE, connection, or cache update PARTIAL. Tests:
`tests/test_producer_compatibility.py` (fixtures in
`tests/fixtures/producer-logs`) and `scripts/run_examples.py --producer both`.

## Known OPEN items

These were observed during review but were not part of the latest legend task:

- `vine_export` advertises PNG/PDF export, but `combine_pngs_to_pdf()` remains a
  placeholder and PDF generation is disabled.
- The README zoom text still emphasizes Ctrl + wheel and does not fully describe
  the newer visible zoom controls.
- The changelog for `2026.8.2` predates several post-release UI commits, so the
  release notes should be reconciled before the next packaged release.
- TaskVine debug and transaction inputs are upstream unversioned formats; keep
  compatibility tests and fail-closed diagnostics when changing parser rules.

Correctness and maintainability take priority over speculative optimization.
Prefer shared implementations, explicit contracts, real-log validation, and
small reversible changes.
