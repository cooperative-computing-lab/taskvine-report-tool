# Acceptance record: 2026.8.2

Date: 2026-08-22  
Result: **PASS**

## Scope

- canonical runtime domains and explicit temporal-series boundaries;
- shared line-grid cursor, active-first ordering, pinning, and keyboard use;
- generic Display CDF round trip;
- parser provenance, distribution contents, and installed-wheel startup.

## Automated gates

- Python unit/API suite: 62/62 passed.
- Playwright browser suite: 9/9 passed.
- JavaScript syntax, Python compilation, and `git diff --check`: passed.
- 120 series x 2,000 points with 500 cursor queries: below the 1.5 second gate.
- sdist and wheel build plus Twine metadata checks: passed.
- clean wheel installation reported `vine_parse 2026.8.2`; a two-thread report
  server started from that wheel and returned a healthy 2026.8.2 response.

## Real-run parser acceptance

Both runs were rebuilt from their raw debug, transaction, and performance logs
with 2026.8.2. The committed manifests and diagnostics agree on the source hash.

| Run | Contract | Outputs | Conservation |
| --- | --- | ---: | --- |
| `lpc_logs` | PARTIAL: manager-end evidence absent | 36 | exact |
| `trijet_1000to1500_71510` | PASS | 37 | exact |

Conservation compared manager duration, task and file row counts, aggregate file
size, cumulative transferred bytes, and cumulative created bytes with the
pre-reparse baseline. No value changed. The incomplete run remains fail-closed;
the reparse did not promote inferred evidence to PASS.

## Real API and browser acceptance

- `lpc_logs`: 11 available temporal metrics and 109 line series all covered the
  canonical runtime domain; four unsupported source-backed metrics remained
  unavailable as required.
- `trijet_1000to1500_71510`: 15 temporal metrics and 116 line series all covered
  the canonical runtime domain.
- Warm real-run API sample: 45 requests, median 4.96 ms, P95 26.91 ms, maximum
  33.45 ms.
- The real 24-line worker chart displayed every value, sorted active values
  before zero values, retained a pinned cursor while crossing another grid, and
  released it with `Escape`. Keyboard pinning also passed.
- Display CDF produced 100 points with a working shared cursor and restored all
  1,000 original file-size points after Undisplay CDF.
- Browser exceptions, API 5xx responses, and service errors: zero.

The deployed login-node service is bound to `127.0.0.1:9122`, uses eight request
threads, and reports version 2026.8.2.
