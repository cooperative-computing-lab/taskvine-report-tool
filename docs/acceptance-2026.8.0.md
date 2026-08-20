# 2026.8.0 production acceptance

Date: 2026-08-20
Scope: TaskVine Report Tool only; TaskVine core unchanged.

## Automated gates

| Gate | Result |
| --- | --- |
| Python unit and integration tests | 32 passed |
| Playwright browser tests | 3 passed |
| JavaScript syntax | 27 modules passed |
| Python compileall | Passed |
| npm high-severity audit | 0 vulnerabilities |
| API contract vs registered routes | Passed |
| Git diff whitespace check | Passed |

Browser coverage includes two independent contexts selecting different runs,
rapid stale-response cancellation, reload state cleanup, revision-safe API
traffic, legend/plot geometry, orange hover, and vertical card resizing. The
browser suite rejects JavaScript exceptions and any API response at status 500
or above.

## Distribution acceptance

- Built `taskvine_report_tool-2026.8.0.tar.gz` and
  `taskvine_report_tool-2026.8.0-py3-none-any.whl`.
- `twine check` passed both distributions.
- The wheel contains the API, snapshot store, runtime metrics, parser source
  adapters, HTML template, and current static assets.
- A new isolated virtual environment installed the wheel and passed
  `vine_parse --version`, `vine_report --version`, `vine_export --version`,
  `vine_report gc --help`, the lazy `DataParser` public import, and application
  import. All three CLIs reported `2026.8.0`.
- The rendered Conda recipe resolves package version `2026.8.0` and Python
  requirement `>=3.10` for both host and runtime.

## Real-log installed-wheel acceptance

The installed wheel served the repository's `lpc_logs` and
`trijet_1000to1500_71510` runs with eight request threads and forty total
active/pending slots.

Concurrent benchmark:

| Measure | Result |
| --- | ---: |
| Clients | 32 |
| Mixed requests | 200 |
| Identity-correct responses | 200 |
| Failures | 0 |
| Throughput | 11.526 requests/s |
| Latency P50 | 2.5355 s |
| Latency P95 | 3.4810 s |
| Maximum latency | 3.9094 s |

The workload alternated both run revisions across summary, task concurrency,
worker concurrency, and file-size chart endpoints and verified JSON identity
plus `X-TaskVine-Run` and `X-TaskVine-Revision` headers.

Two simultaneous real-browser contexts loaded different real runs from the
installed wheel. Both retained the correct selection, `lpc_logs` rendered 377
file-size points, and no JavaScript or API 500 errors occurred.

Operator diagnostics after the benchmark reported peak active requests `8`,
completed requests `205`, server errors `0`, compute threads `4`, and no
absolute filesystem path in the payload. Applying GC while the installed-wheel
server was active failed closed with `gc_server_active` and exit status `2`.

## Known evidence boundary

Both real runs were produced before parser diagnostics and parse manifests were
introduced, so diagnostics correctly report their data quality as `UNKNOWN`.
This is not a concurrency or snapshot-integrity failure. Re-running
`vine_parse` with 2026.8.0 is required to produce PASS/PARTIAL compatibility
evidence.

PyPI upload, Conda upload, Git commit, push, and release tagging are not part of
this acceptance because they require an explicit credentialed publication
decision.
