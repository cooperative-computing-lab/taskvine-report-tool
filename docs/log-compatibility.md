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

The companion `transactions` file remains unversioned. It is used to validate
its own record structure, to infer the debug wall-clock offset from the
manager-start epoch when necessary, to name each retrieval's final result, and
to reconcile the debug model (see "Reconciliation with transactions"). It is
not silently used as a complete replacement data source.

Operational charts use a narrower metric contract: transaction task attempts,
worker sessions, and transfers are reconciled with debug-derived task
classification, while manager capacity samples come from `performance`. Each
metric is independently versioned and fail-closed; `PARTIAL` and `UNAVAILABLE`
contracts are diagnostic evidence, not plot-ready data.

## Supported producers

Two producers are supported and tested by the example suite
(`scripts/run_examples.py`, see the README):

- **upstream** cooperative-computing-lab/cctools master. Each field of a task
  description is its own `tx to <host> (<addr>): <field>` record, and the
  command after `cmd <n>` is a separate `vine: <command>` record.
- **vine-graph** fork. A task description is one debug record whose later
  fields follow on untimestamped lines; the command body follows `cmd <n>`
  without a newline of its own (`cmd 8` then `run_nodeneeds_library ...`).

`sources.expand_task_descriptions()` turns both shapes into the upstream one
before classification. It skips the bodies announced by `cmd <n>` and
`env <n>` by byte length, so a command that spans lines, lacks a trailing
newline, or contains text such as `complete`, `state change:`, `end`, or a
timestamp is never read as a record. Each physical line still yields one
record, so line numbers and source hashes are unchanged. While a description
is open, its fields go to the task-description reducer by their exact leading
key (`cores`, `infile`, ...), so a file named `stdout` or `put` cannot be
claimed by another family, and a new `task <id>` head closes a description
whose `end` was never logged.

Message families of the fork, each with its own handler and strict patterns:

| Handler | Records | Model |
| --- | --- | --- |
| `puturl` | `puturl manager://<cache name> ...` | transfer from the Manager's vault (`origin=manager_vault`) |
| `checkpoint` | `checkpoint started/complete/failed/cancelled`, and known notes (`queued`, `withdrawn`, `waits for readers`, `connected back`, `cut`, `of X waits/waited`, `<known name>: <reason>`) | `manager.checkpoints`, a transfer class of its own (worker to Manager, never a cache residency) |
| `simulated_loss` | `Losing the state of worker ... on request`, `tx ... lose <n>`, `rx ... lost <n>`, `worker ... lost its state` | `manager.simulated_losses`; the worker's residencies end (`simulated_loss`), its cache messages are dropped until `lost its state`, and its tasks are charged as worker-disconnected |
| `task_lost_worker` | `task <id> lost worker ... : k of m tries used` | the attempt is worker-disconnected |
| `fork_info` | `round summary:`, `tx ... connect-back <token>`, `vault hit:`, `dropped a stale message` | owned, not modeled |

An unknown `puturl` source scheme, a `checkpoint` record of no known shape, a
checkpoint end without a start, and a `put` record that is neither
`tx ... put <name> <level> <size>` nor a known failure notice all fail the
parse with line and handler evidence.

Both producers: a removed worker's residencies end at its removal
(`worker_removed`), as the Manager forgets them then. A `cache-update` with
transfer id `X` completes the put or mini task to that worker that is still in
flight, also for a file a task produced; only without one does it open a new
`task_output` residency. (Before, a task output put to another worker was
counted there twice, as a put that never completed and as a task output,
which broke `worker-cache-occupancy` reconciliation in short runs.)

Records of both producers with handlers of their own:

| Handler | Records | Model |
| --- | --- | --- |
| `worker_rejected` | `worker <addr> presented the wrong password` (both), `worker <host> (<addr>) opened its probe link from <addr2>` (fork timeout framework) | the session opened by `worker <addr> connected` is retracted (`manager.retracted_connections`), since it is no `WORKER CONNECTION`; a retraction of an unknown address or of a session with state fails the parse |
| `send_task_to_worker` | `tx ... mini_task <remote> <cache name> <level> <size> <mode>` and its fields up to `end` | a transfer of origin `mini_task` to that worker, ended by the file's `cache-update`; the mini task's fields are not fields of any task. A malformed head or an unknown field fails the parse |

## Option coverage

The `opt_*` workloads in `examples/workloads` turn on Manager options that
change what the Manager logs. Each runs on both producers unless marked.

| Workload | Options |
| --- | --- |
| `opt_tune_knobs` | 25 `tune()` knobs at unusual values: `perf-log-interval` 1, `hungry-minimum`, `wait-for-workers`, `worker-retrievals`, `max-retrievals`, `transient-error-interval`, keepalive interval and timeout, `temp-replica-count`, `file-source-max-transfers`, `immediate-recovery`, `attempt-schedule-depth`, `prefer-dispatch`, `monitor-interval`, ... |
| `opt_temp_replicas` | peer transfers, `temp-replica-count` 3 on three workers, readers, `remove_file()` |
| `opt_monitoring` | `enable_monitoring(watchdog, time_series)`, a snapshot file |
| `opt_categories` | categories in `fixed`, `max`, `min waste`, `max throughput` modes, first allocation guesses, a memory exhaustion retried at the maximum |
| `opt_watch_files` | watched (streamed) output files |
| `opt_ssl` | `Manager(ssl=True)`, worker `--ssl` |
| `opt_password` | password file, one worker with the wrong password (`worker_rejected`) |
| `opt_catalog_name` | Manager name, an unreachable catalog, `update-interval` 1 |
| `opt_slow_workers` | `enable_disconnect_slow_workers(2)` with an outlier task (fast abort, retries) |
| `opt_priorities_tags` | Manager and per-task schedulers, priorities, tags with spaces and unicode, retries |
| `opt_blocklist` | `block_host()`, `unblock_host()`, `set_draining_by_hostname()` |
| `opt_minitask` | `declare_untar()` and `declare_minitask()` (`mini_task` records) |
| `opt_weird_commands` | a 60 KB command line, unicode, newlines, environment variables with newlines, 3 MB stdout, empty stdout, stdout and stderr that imitate debug records |
| `opt_stdout_limits` | `max-task-stdout-storage` 1000 with 5 MB binary, empty, and unterminated stdout |
| `opt_stdout_nul` (fork only) | stdout with NUL bytes; upstream master never returns such a task (see below) |
| `opt_worker_options` | workers with `--single-shot`, `--idle-timeout`, `--disk-percent`, `--wall-time`, resource overrides, three per host |
| `opt_features` | worker `--feature`, tasks requiring them, a task no worker can run (cancelled) |
| `opt_library_direct` | a `direct` library with one slot, `remove_library()`, a `fork` library, `watch-library-logfiles` |
| `opt_disk_allocation` | per-task disk, `disk-proportion-available-to-task` |
| `opt_proportional` | `force-proportional-resources` on whole tasks |
| `opt_timeout_framework` (fork only) | `response-timeout`, `manager-response-timeout`, worker `--response-timeout` (probe links) |

Not covered: the Manager's debug log has no size limit or rotation through the
Python API (`vine_enable_debug_log()` takes only a path), worker debug logs are
not parsed, and `load-from-shared-filesystem` with plain local paths makes
workers fail every input (`cache-invalid`) on both builds, so its tasks never
finish.

Producer defects seen while writing these workloads (both builds unless
noted): `Manager.block_host_with_timeout()` fails in SWIG (`time_t`),
`Manager.set_category_max_concurrent()` refers to `self._work_queue`,
`Task.set_snapshot_file()` passes a path where C wants a `vine_file`,
`Task.set_scheduler()` prefixes `VINE_` twice so no name works, and a library
with more function slots than cores fails at start ("function slots cannot be
more than library cores") and its message makes the Manager drop the worker
("Invalid message from worker"). Upstream only: a task whose stdout holds NUL
bytes is never returned ("actual received stdout size (-1 bytes)" after 60 s).

## Transfer classes

`parse-diagnostics.json` has `transfer_summary`, and
`csv-files/transfer_classes.csv` the same table: completed transfers by class
with counts and bytes. `manager_put` (Manager to worker put), `peer`
(`workerip://`), `url` (`http(s)://`, `ftp://`, `file://`, `root://`),
`manager_vault` (`manager://`), and `task_output` (created by a task) end in a
worker cache and count in `file_transferred_size.csv`. `checkpoint` and
`checkpoint_inline` move worker data into the vault and are listed one per
receive in `csv-files/checkpoint_transfers.csv`.

## Reconciliation with transactions

After the debug pass, and before any synthetic end times are filled in, the
parser compares its model with the transactions records after the last
`MANAGER START`:

| Check | Transactions | Debug model |
| --- | --- | --- |
| `dispatches` | `TASK <id> RUNNING` records | attempts with a run time and a worker |
| `dispatch_attribution` | worker id of every RUNNING record, per task | worker hash of every dispatched attempt |
| `dispatch_cores` | allocated `cores` of the k-th RUNNING record | requested cores of the k-th dispatched attempt (standard tasks) |
| `done_tasks` | task ids with `DONE` | task ids with a done time |
| `worker_connections` | `WORKER ... CONNECTION` | worker sessions |
| `cache_updates` | `WORKER ... CACHE_UPDATE` | replicas with a stage-in time |

Any mismatch adds `reconcile_<check>_mismatch` to `partial_reasons`, so a
parse that lost task descriptions, attributions, or replicas is never `PASS`.
Results are in `parse-diagnostics.json` under `reconciliation`.

The debug log states a result only when the worker reports one (`complete`).
A cancelled task, a task out of retries, or a task the Manager fails itself is
only retrieved, so the n-th `TASK <id> RETRIEVED <RESULT>` record sets the
result of the task's n-th retrieved attempt (`transaction_results` in the
diagnostics). Library instances keep the parser's `library-exit` label. A
task whose retrieval counts differ adds `reconcile_retrievals_mismatch`.

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
- `python scripts/run_examples.py --producer both` passes on current builds of
  both producers;
- dashboard API and badge surface `PASS`, `PARTIAL`, or `UNKNOWN` data quality.

This design prevents silent breakage and makes drift diagnosable. It cannot
guarantee compatibility with arbitrary future debug text; that guarantee would
require a versioned producer contract in TaskVine itself.
