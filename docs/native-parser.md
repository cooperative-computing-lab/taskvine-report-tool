# Native debug classifier

The parser keeps one authoritative, ordered Python reducer.  An optional C
extension performs only stateless candidate classification in batches; it does
not mutate tasks, workers, files, or parser context.

## Boundary

1. `LegacyDebugSource` preserves bytes, decoded text, and line numbers.
2. `CandidateClassifier` groups up to 4096 decoded lines and returns a packed
   `uint32` candidate mask for each line.
3. `DataParser` applies the original handler priority and all state-dependent
   conditions in line order.
4. A missing extension returns `None` masks and executes the reference handler
   loop without changing behavior.
5. A native mask that excludes every diagnostic protocol family safely skips
   the broader unhandled-line regex; fallback mode retains the original scan.

The C rules are a conservative prefilter.  `worker_resources` remains
state-dependent in Python, and `send_task_to_worker` is intentionally
classified from the broader `tx to` discriminator before Python validates its
task context.

## Compatibility controls

- Python and C expose the same ordered handler table and ruleset version.
- Import fails closed on a table or version mismatch.
- Unit tests compare the C mask with `reference_candidate_mask` over protocol
  fixtures, overlapping patterns, embedded NULs, and Unicode text.
- Release acceptance compares every line of supported real logs and requires
  identical handler counts, model serialization, shape sets, and format
  fingerprints between native and reference paths.
- The extension is optional at build time, so environments without a compiler
  retain the Python parser.
- `TASKVINE_DISABLE_NATIVE_PARSER=1` is an operational kill switch that forces
  the reference path without uninstalling the extension.
- Package update checks are opt-in through `TASKVINE_CHECK_UPDATES=1`; parsing
  never performs an unrequested synchronous network check by default.

Run the repeatable benchmark with:

```bash
python scripts/benchmark_parser.py logs/lpc_logs --mode both \
  --expected-fingerprint 91a919ac873aed16b4dbab7c962f3eb5e77e94fe61b5bdca4acff004527de78a
```
