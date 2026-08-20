# Log compatibility contract

TaskVine Report Tool currently consumes TaskVine's unversioned debug text. The
tool cannot make that upstream format stable without changing TaskVine core, so
its local contract is deliberately fail-closed: isolate the format, identify
what was understood, preserve evidence, and never report an incompatible or
truncated parse as fully healthy.

## Ownership boundary

- `sources/debug_legacy.py` owns byte decoding and line identity for the debug
  format.
- `DataParser` owns mappings from recognized legacy messages to report domain
  objects.
- `diagnostics.py` owns source fingerprints, compatibility evidence, status,
  transaction-log inspection, and output traceability.
- Dashboard code consumes generated models and exposes only the bounded
  `data_quality` summary. It does not parse TaskVine messages.

The companion `transactions` file remains unversioned and is evidence only. It
is used to validate its own record structure and, when necessary, to infer the
debug wall-clock offset from the manager-start epoch. It is not silently used as
a complete replacement data source.

## Status and exit-code contract

| Status | Meaning | `vine_parse` exit |
| --- | --- | --- |
| `PASS` | Parsing and projection completed with complete available evidence | `0` |
| `PARTIAL` | Outputs were generated, but completeness cannot be proven | `2` |
| `FAIL` | Input is missing, incompatible, corrupt, or projection failed | `1` |

Examples of `PARTIAL` include a missing manager-end event, undecodable records,
or a recognized protocol-family line for which no handler claimed ownership.
An exception inside a recognized handler is `FAIL` and includes the source line
number and handler name.

## Operator workflow

Run a non-destructive preflight before a large parse:

```bash
vine_parse --doctor /path/to/run
```

Every parse writes:

- `vine-logs/parse-diagnostics.json`: source hash, adapter, format fingerprint,
  handler coverage, unknown protocol candidates, compatibility evidence,
  transaction inventory, partial reasons, and bounded sanitized errors.
- `csv-files/parse-manifest.json`: source identity plus size and SHA-256 for
  every generated CSV.

Timezone evidence is selected in this order:

1. the debug log's numeric `tz_offset` metadata;
2. a safe inference from debug manager-start wall time and transaction-log
   manager-start epoch;
3. an explicit operator value, for example `--timezone-offset -0500`.

An explicit value that conflicts with embedded metadata is rejected.

## Compatibility change procedure

When a new TaskVine release fails:

1. Save the raw debug and transactions files unchanged.
2. Run `vine_parse --doctor RUN_DIR` and attach `parse-diagnostics.json`.
3. Minimize the first failing record into a test fixture.
4. Add a new source adapter for a genuinely new format, or update the legacy
   adapter/handler only when the change is compatible.
5. Keep old fixtures and adapters until their documented support window ends.
6. Require the acceptance matrix below before release.

Do not fix drift by adding an untested broad substring fallback.

## Release acceptance matrix

A compatibility change is accepted only when all of these pass:

- all unit and dashboard tests;
- valid debug metadata, transaction-inferred timezone, and explicit timezone;
- malformed recognized message fails with line and handler evidence;
- missing timezone evidence is not treated as `PASS`;
- truncated manager log is `PARTIAL`, not healthy;
- malformed transaction records are counted;
- CLI returns `0`, `2`, and `1` according to the table above;
- output manifest hashes verify against every generated CSV;
- at least one real log from each supported CCTools line completes parsing and
  CSV projection with zero unhandled protocol candidates;
- dashboard API and badge surface `PASS`, `PARTIAL`, or `UNKNOWN` data quality.

This design prevents silent breakage and makes drift diagnosable. It cannot
guarantee compatibility with arbitrary future debug text; that guarantee would
require a versioned producer contract in TaskVine itself.
