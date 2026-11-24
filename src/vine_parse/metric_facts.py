"""Source-backed observability facts and metric contracts.

The legacy debug parser remains responsible for detailed TaskVine objects.  This
module deliberately derives operational metrics from the less ambiguous
transaction and performance streams, then reconciles them with parser objects.
No chart is allowed to invent a missing terminal event or resource sample.
"""

from __future__ import annotations

import bisect
import csv
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path

from .diagnostics import sha256_file
from .file_info import UnindexedTransferEvent
from .sources import LegacyTransactionSource


METRIC_SEMANTICS_VERSION = 1
METRIC_CONTRACT_FILENAME = "metric_contracts.csv"
_JSON_OBJECT_RE = re.compile(r"\{[^{}]*\}")


def _finite(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _resource_value(resources: dict, key: str):
    value = resources.get(key)
    if isinstance(value, list) and value:
        return _finite(value[0])
    return _finite(value)


def _last_json(payload: str) -> dict:
    matches = _JSON_OBJECT_RE.findall(payload)
    if not matches:
        return {}
    try:
        value = json.loads(matches[-1])
    except (TypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows({key: row.get(key) for key in fields} for row in rows)


def _bucket_rows(events, start_epoch, end_epoch, value_keys, max_points=400):
    duration = max(0.0, end_epoch - start_epoch)
    width = max(1.0, math.ceil(duration / max_points))
    bucket_count = max(1, math.ceil(duration / width))
    buckets = defaultdict(lambda: defaultdict(float))
    for timestamp, values in events:
        offset = max(0.0, timestamp - start_epoch)
        index = min(bucket_count - 1, int(offset // width))
        for key, value in values.items():
            buckets[index][key] += value
    rows = []
    for index in range(bucket_count):
        row = {"time": round(index * width, 6), "bucket_seconds": width}
        for key in value_keys:
            row[key] = buckets[index].get(key, 0.0)
        rows.append(row)
    return rows


def _workflow_pressure_rows(attempts, start_epoch, end_epoch):
    states = ("queued", "executing", "retrieving")
    deltas = defaultdict(Counter)
    observed = 0
    for fact in attempts:
        ready = fact.get("ready")
        if ready is None:
            continue
        observed += 1
        current, last = "queued", ready
        deltas[ready][current] += 1
        for field, next_state in (("running", "executing"), ("waiting_retrieval", "retrieving")):
            timestamp = fact.get(field)
            if timestamp is None or timestamp < last:
                continue
            deltas[timestamp][current] -= 1
            deltas[timestamp][next_state] += 1
            current, last = next_state, timestamp
        terminal = fact.get("terminal") or fact.get("done")
        if terminal is not None and terminal >= last:
            deltas[terminal][current] -= 1

    current = Counter({state: 0 for state in states})
    rows = []
    for timestamp in sorted(deltas):
        for state, delta in deltas[timestamp].items():
            current[state] += delta
        rows.append({
            "time": round(max(0.0, timestamp - start_epoch), 6),
            **{state: current[state] for state in states},
        })
    if not rows or rows[0]["time"] > 0:
        rows.insert(0, {"time": 0.0, **{state: 0 for state in states}})
    duration = max(0.0, end_epoch - start_epoch)
    if rows[-1]["time"] < duration:
        rows.append({"time": duration, **{state: current[state] for state in states}})
    return rows, observed


def _retry_pressure_rows(attempts, start_epoch, end_epoch):
    # Retry is an application-level concept. Internal library/recovery tries
    # may precede the first application try for the same task id, so the raw
    # transaction attempt number cannot classify the filtered sequence.
    first_application_attempt = {}
    for fact in attempts:
        task_id = fact["task_id"]
        first_application_attempt[task_id] = min(
            fact["attempt"], first_application_attempt.get(task_id, fact["attempt"])
        )
    events = []
    for fact in attempts:
        ready = fact.get("ready")
        if ready is None:
            continue
        is_first = fact["attempt"] == first_application_attempt[fact["task_id"]]
        events.append((ready, {
            "first_attempts": float(is_first),
            "retries": float(not is_first),
        }))
    rows = _bucket_rows(events, start_epoch, end_epoch, ["first_attempts", "retries"])
    for row in rows:
        row["first_attempt_rate_per_second"] = row["first_attempts"] / row["bucket_seconds"]
        row["retry_rate_per_second"] = row["retries"] / row["bucket_seconds"]
    return rows


def _worker_event_rows(events, start_epoch, end_epoch):
    reasons = sorted({"CONNECTED" if delta > 0 else reason for _, _, delta, reason in events})
    if not reasons:
        return []
    duration = max(0.0, end_epoch - start_epoch)
    width = max(1.0, math.ceil(duration / 400))
    bucket_count = max(1, math.ceil(duration / width))
    buckets = defaultdict(Counter)
    for timestamp, _, delta, reason in events:
        index = min(bucket_count - 1, int(max(0.0, timestamp - start_epoch) // width))
        buckets[index]["CONNECTED" if delta > 0 else reason] += 1
    return [
        {
            "time": round(index * width, 6), "bucket_seconds": width,
            "reason": reason, "count": buckets[index][reason],
            "events_per_second": buckets[index][reason] / width,
        }
        for index in range(bucket_count)
        for reason in reasons
    ]


DEBUG_TIME_RESOLUTION = 0.01


def _step_function(deltas):
    """Return (times, totals) of the running sum of ``deltas``, floored at zero."""
    times = sorted(deltas)
    totals = []
    current = 0.0
    for timestamp in times:
        current = max(0.0, current + deltas[timestamp])
        totals.append(current)
    return times, totals


def _value_at(step, x):
    times, totals = step
    index = bisect.bisect_right(times, x) - 1
    return totals[index] if index >= 0 else 0.0


def _window_bound(step, signed, start, end, side):
    """Bound the step function over the ambiguous window (start, end].

    Events inside the window may have happened in any order, so the lowest
    reachable value is the value at ``start`` plus every decrease in the
    window, and the highest is that value plus every increase.
    """
    times = signed[0]
    low = bisect.bisect_right(times, start)
    high = bisect.bisect_right(times, end)
    index = 0 if side == "low" else 1
    return max(0.0, _value_at(step, start) + sum(signed[1][i][index] for i in range(low, high)))


def _signed_deltas(pairs):
    """Return (times, [(decrease, increase)]) from {time: [decrease, increase]}."""
    times = sorted(pairs)
    return times, [tuple(pairs[t]) for t in times]


class MetricFactBuilder:
    """Build immutable metric projections and explicit quality contracts."""

    def __init__(self, runtime_template: str, data_parser):
        self.run = Path(runtime_template)
        self.logs = self.run / "vine-logs"
        self.csv_dir = self.run / "csv-files"
        self.dp = data_parser
        self.contracts = []
        self.partial_reasons = []
        self.provenance = {}

    def _contract(
        self,
        metric_id,
        status,
        source,
        coverage,
        unit,
        inclusion,
        exclusions,
        caveat="",
        status_reason="",
    ):
        self.contracts.append({
            "metric_id": metric_id,
            "semantics_version": METRIC_SEMANTICS_VERSION,
            "status": status,
            "source": source,
            "coverage": round(float(coverage), 6),
            "unit": unit,
            "inclusion": inclusion,
            "exclusions": exclusions,
            "caveat": caveat,
            "status_reason": status_reason,
        })

    def _downgrade_contract(self, metric_id, status_reason):
        for contract in self.contracts:
            if contract["metric_id"] != metric_id:
                continue
            contract["status"] = "PARTIAL"
            existing = contract.get("status_reason", "")
            contract["status_reason"] = "; ".join(
                value for value in (existing, status_reason) if value
            )
            return

    def _task_classification(self, task_id, attempt, terminal_status):
        task = self.dp.tasks.get((task_id, attempt)) if self.dp is not None else None
        is_library = bool(getattr(task, "is_library_task", False))
        is_recovery = bool(getattr(task, "is_recovery_task", False))
        if terminal_status == "LIBRARY_EXIT":
            is_library = True
        return is_library, is_recovery, task is not None

    def _transactions(self):
        path = self.logs / "transactions"
        if not path.is_file():
            return None
        records = list(LegacyTransactionSource(str(path)).records())
        malformed = sum(record.error is not None for record in records)
        self.provenance["transactions"] = {
            "path": str(path.resolve()),
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
            "records": len(records),
            "malformed_records": malformed,
            "adapter": LegacyTransactionSource.adapter_id,
        }
        if malformed:
            self.partial_reasons.append("metric_transactions_malformed")
        return records

    def _performance(self):
        path = self.logs / "performance"
        if not path.is_file():
            return None
        rows = []
        malformed = 0
        header = None
        with path.open(encoding="utf-8", errors="replace") as handle:
            for raw in handle:
                line = raw.strip()
                if not line:
                    continue
                if line.startswith("#"):
                    candidate = line.lstrip("# ").split()
                    if candidate and candidate[0] == "timestamp":
                        header = candidate
                    continue
                parts = line.split()
                if header is None or len(parts) != len(header):
                    malformed += 1
                    continue
                try:
                    rows.append({key: float(value) for key, value in zip(header, parts)})
                except ValueError:
                    malformed += 1
        self.provenance["performance"] = {
            "path": str(path.resolve()),
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
            "records": len(rows),
            "malformed_records": malformed,
            "format": "taskvine.performance.unversioned",
        }
        if malformed:
            self.partial_reasons.append("metric_performance_malformed")
        return rows

    def build(self):
        transaction_records = self._transactions()
        performance_rows = self._performance()
        if transaction_records is None:
            for metric_id in (
                "workflow-pressure", "retry-pressure", "task-throughput", "task-latency", "failure-timeline",
                "worker-churn", "transfer-throughput",
            ):
                self._contract(
                    metric_id, "UNAVAILABLE", "transactions", 0, "", "", "all",
                    status_reason="Transactions source file is missing.",
                )
            self._write_empty_transaction_outputs()
            start_epoch = None
        else:
            start_epoch = self._build_transaction_metrics(transaction_records)
        self._build_capacity(performance_rows, start_epoch)
        self._build_cache_occupancy(performance_rows)
        _write_csv(
            self.csv_dir / METRIC_CONTRACT_FILENAME,
            ["metric_id", "semantics_version", "status", "source", "coverage", "unit", "inclusion", "exclusions", "caveat", "status_reason"],
            self.contracts,
        )
        return {
            "semantics_version": METRIC_SEMANTICS_VERSION,
            "sources": self.provenance,
            "partial_reasons": sorted(set(self.partial_reasons)),
            "contracts": {row["metric_id"]: row for row in self.contracts},
        }

    def _build_cache_occupancy(self, performance_rows):
        """Project cache residency deltas and reconcile them with inuse_cache.

        Debug cache lifecycle messages remain the per-worker source of truth.
        The manager performance stream is an independent aggregate check; it
        cannot replace debug because it has no worker or file dimensions.
        """
        fields = [
            "time", "worker_id", "delta_bytes", "file_id", "file_name",
            "residency_id", "reason", "evidence",
        ]
        quality_fields = [
            "eligible_residencies", "projected_residencies", "right_censored",
            "coverage", "performance_samples", "median_relative_error",
            "peak_relative_error",
        ]
        if self.dp is None or not hasattr(self.dp, "files"):
            _write_csv(self.csv_dir / "metric_worker_cache_occupancy.csv", fields, [])
            _write_csv(self.csv_dir / "metric_worker_cache_occupancy_quality.csv", quality_fields, [])
            return

        try:
            base_time, _ = self.dp.manager.get_min_max_time()
            base_time = float(base_time)
        except (AttributeError, TypeError, ValueError):
            base_time = None
        events = []
        eligible = projected = censored = 0
        aggregate_deltas = defaultdict(float)
        inflight_deltas = defaultdict(float)
        cached_signed = defaultdict(lambda: [0.0, 0.0])
        inflight_signed = defaultdict(lambda: [0.0, 0.0])
        for file in self.dp.files.values():
            for index, transfer in enumerate(file.get_flattened_transfers(), start=1):
                stage_in = transfer.time_stage_in
                start_inferred = False
                if (
                    stage_in is None
                    and isinstance(transfer, UnindexedTransferEvent)
                    and transfer.end_reason != "cache_invalid"
                    and transfer.time_start_stage_in is not None
                ):
                    # Older TaskVine (e.g. 7.16) sends no cache-update after a
                    # direct put; the manager counts the replica once it is sent.
                    stage_in = transfer.time_start_stage_in
                    start_inferred = True
                if stage_in is None:
                    continue
                eligible += 1
                size_mb = getattr(transfer, "size_mb", None)
                if size_mb is None:
                    size_mb = getattr(file, "size_mb", None)
                if (
                    base_time is None or transfer.time_stage_out is None
                    or size_mb is None or float(size_mb) < 0
                ):
                    continue
                projected += 1
                reason = getattr(transfer, "end_reason", None) or "unknown"
                is_censored = reason == "synthetic_manager_end"
                censored += int(is_censored)
                size_bytes = int(round(float(size_mb) * 2**20))
                worker_id = ":".join(map(str, transfer.dest_worker_entry))
                residency_id = f"{file.file_idx}:{index}"
                start = max(0.0, float(stage_in) - base_time)
                end = max(start, float(transfer.time_stage_out) - base_time)
                events.extend((
                    {
                        "time": start, "worker_id": worker_id,
                        "delta_bytes": size_bytes, "file_id": file.file_idx,
                        "file_name": file.filename, "residency_id": residency_id,
                        "reason": "put" if start_inferred else "cache_update",
                        "evidence": "inferred" if start_inferred else "observed",
                    },
                    {
                        "time": end, "worker_id": worker_id,
                        "delta_bytes": -size_bytes, "file_id": file.file_idx,
                        "file_name": file.filename, "residency_id": residency_id,
                        "reason": reason,
                        "evidence": "inferred" if is_censored else "observed",
                    },
                ))
                aggregate_deltas[start] += float(size_mb)
                aggregate_deltas[end] -= float(size_mb)
                cached_signed[start][1] += float(size_mb)
                cached_signed[end][0] -= float(size_mb)
                # TaskVine counts a replica from the moment its transfer
                # starts; the projection counts it once it is cached.
                started = getattr(transfer, "time_start_stage_in", None)
                inflight_start = start if started is None else min(start, max(0.0, float(started) - base_time))
                inflight_deltas[inflight_start] += float(size_mb)
                inflight_deltas[end] -= float(size_mb)
                inflight_signed[inflight_start][1] += float(size_mb)
                inflight_signed[end][0] -= float(size_mb)
        events.sort(key=lambda row: (row["time"], row["worker_id"], row["residency_id"], -row["delta_bytes"]))
        _write_csv(self.csv_dir / "metric_worker_cache_occupancy.csv", fields, events)

        coverage = projected / eligible if eligible else 0.0
        performance_samples = 0
        median_relative_error = None
        peak_relative_error = None
        reconciliation_ok = False
        comparable = [
            row for row in (performance_rows or [])
            if "timestamp" in row and "inuse_cache" in row and base_time is not None
        ]
        if comparable and not aggregate_deltas:
            # No cache residency at all: the evidence agrees only if TaskVine
            # never reported cache payload either.
            observed = [float(row["inuse_cache"]) for row in comparable]
            performance_samples = len(observed)
            median_relative_error = peak_relative_error = max(observed, default=0.0)
            reconciliation_ok = all(value == 0 for value in observed)
        elif comparable:
            cached = _step_function(aggregate_deltas)
            inflight = _step_function(inflight_deltas)
            cached_bounds = _signed_deltas(cached_signed)
            inflight_bounds = _signed_deltas(inflight_signed)
            first_event, last_event = cached[0][0], cached[0][-1]
            observed = []
            expected = []
            consistent = 0
            for row in comparable:
                relative = row["timestamp"] / 1e6 - base_time
                if relative < first_event or relative > last_event:
                    continue
                # Debug timestamps are floored to 10 ms, so an event logged at
                # d happened in [d, d + 0.01): events logged in (t - 0.01, t]
                # may or may not precede the sample, in any order.  The value
                # TaskVine reports lies between the cached-only total and the
                # total that also counts transfers in flight, over that window.
                window = relative - DEBUG_TIME_RESOLUTION
                low = _window_bound(cached, cached_bounds, window, relative, "low")
                high = _window_bound(inflight, inflight_bounds, window, relative, "high")
                value = float(row["inuse_cache"])
                # TaskVine reports inuse_cache as ceil(bytes / 2**20).
                expected.append(float(math.ceil(_value_at(cached, relative) - 1e-9)))
                observed.append(value)
                consistent += math.ceil(low - 1e-9) <= value <= math.ceil(high - 1e-9)
            performance_samples = len(observed)
            if observed:
                relative_errors = sorted(
                    abs(left - right) / max(abs(right), 1.0)
                    for left, right in zip(expected, observed)
                )
                middle = len(relative_errors) // 2
                median_relative_error = (
                    relative_errors[middle] if len(relative_errors) % 2
                    else (relative_errors[middle - 1] + relative_errors[middle]) / 2
                )
                peak_relative_error = abs(max(expected) - max(observed)) / max(max(observed), 1.0)
                # Agreement is shown either by the original tolerance on the
                # point estimate (long runs) or by nearly every sample lying in
                # the envelope that 10 ms debug resolution leaves (short runs,
                # where several events share one timestamp).
                reconciliation_ok = (
                    (median_relative_error <= 0.01 and peak_relative_error <= 0.02)
                    or consistent >= 0.99 * len(observed)
                )

        quality = {
            "eligible_residencies": eligible,
            "projected_residencies": projected,
            "right_censored": censored,
            "coverage": coverage,
            "performance_samples": performance_samples,
            "median_relative_error": median_relative_error,
            "peak_relative_error": peak_relative_error,
        }
        _write_csv(
            self.csv_dir / "metric_worker_cache_occupancy_quality.csv",
            quality_fields, [quality],
        )
        complete = bool(getattr(self.dp, "manager_end_observed", False))
        status = "PASS" if coverage >= 0.995 and complete and reconciliation_ok else "PARTIAL"
        caveats = ["TaskVine cache payload only; not physical disk utilization"]
        status_reasons = []
        if coverage < 0.995:
            status_reasons.append(f"Only {coverage:.1%} of eligible cache residencies were projected.")
        if not complete:
            status_reasons.append("Manager END is missing; cache residency completeness cannot be verified.")
        if censored:
            caveats.append(f"{censored} right-censored residencies")
        if not comparable or not performance_samples:
            # Short runs may log no performance sample inside the residency
            # window; that is missing evidence, not a disagreement.
            status_reasons.append("Performance inuse_cache evidence is unavailable.")
        elif not reconciliation_ok:
            status_reasons.append("Performance inuse_cache reconciliation is outside tolerance.")
            self.partial_reasons.append("metric_cache_occupancy_mismatch")
        self._contract(
            "worker-cache-occupancy", status,
            "debug-cache-lifecycle+performance-reconciliation", coverage,
            "bytes", "cache-update to cache-invalid/unlink residencies",
            "sandbox,temp,logs,filesystem-overhead", "; ".join(caveats),
            "; ".join(status_reasons),
        )

    def _write_empty_transaction_outputs(self):
        _write_csv(self.csv_dir / "metric_workflow_pressure.csv", ["time", "queued", "executing", "retrieving"], [])
        _write_csv(self.csv_dir / "metric_retry_pressure.csv", ["time", "bucket_seconds", "first_attempts", "retries", "first_attempt_rate_per_second", "retry_rate_per_second"], [])
        _write_csv(self.csv_dir / "metric_task_throughput.csv", ["time", "bucket_seconds", "successful", "failed", "success_rate_per_second", "failure_rate_per_second"], [])
        _write_csv(self.csv_dir / "metric_task_latency.csv", ["task_id", "attempt", "category", "outcome", "queue_seconds", "execution_seconds", "retrieval_seconds"], [])
        _write_csv(self.csv_dir / "metric_failure_timeline.csv", ["time", "status", "count"], [])
        _write_csv(self.csv_dir / "metric_worker_churn.csv", ["time", "bucket_seconds", "reason", "count", "events_per_second"], [])
        _write_csv(self.csv_dir / "metric_transfer_throughput.csv", ["time", "bucket_seconds", "input_bytes", "output_bytes", "input_bytes_per_second", "output_bytes_per_second"], [])

    def _build_transaction_metrics(self, records):
        starts = []
        ends = []
        current_attempt = defaultdict(int)
        attempts = {}
        worker_events = []
        transfer_events = []
        malformed_payloads = 0

        for record in records:
            if record.timestamp_us is None:
                continue
            timestamp = record.timestamp_us / 1e6
            fields = record.payload.split()
            if record.entity == "MANAGER" and len(fields) >= 2:
                if fields[1] == "START":
                    starts.append(timestamp)
                elif fields[1] == "END":
                    ends.append(timestamp)
                continue
            if record.entity == "WORKER" and len(fields) >= 2:
                action = fields[1]
                worker_id = fields[0]
                if action == "CONNECTION":
                    worker_events.append((timestamp, worker_id, 1, "CONNECTED"))
                elif action == "DISCONNECTION":
                    reason = fields[2] if len(fields) > 2 else "UNKNOWN"
                    worker_events.append((timestamp, worker_id, -1, reason))
                elif action == "TRANSFER" and len(fields) >= 7:
                    try:
                        direction = fields[2]
                        size_bytes = float(fields[4])
                        wall_time_us = float(fields[5])
                        transfer_events.append((timestamp, direction, size_bytes, wall_time_us))
                    except ValueError:
                        malformed_payloads += 1
                continue
            if record.entity != "TASK" or len(fields) < 2:
                continue
            try:
                task_id = int(fields[0])
            except ValueError:
                malformed_payloads += 1
                continue
            state = fields[1]
            if state in {"READY", "WAITING"}:
                try:
                    json_start = next(i for i, value in enumerate(fields) if value.startswith("{"))
                    resource_attempt = int(fields[json_start - 1])
                    category = fields[2]
                except (StopIteration, ValueError, IndexError):
                    malformed_payloads += 1
                    continue
                previous_attempt = current_attempt.get(task_id, 0)
                if previous_attempt:
                    previous = attempts.get((task_id, previous_attempt))
                    if previous is not None and not previous.get("outcome"):
                        parsed = self.dp.tasks.get((task_id, previous_attempt)) if self.dp is not None else None
                        status_name = getattr(parsed, "task_status_name", None) or "unsuccessful-unknown"
                        previous["outcome"] = status_name.removeprefix("unsuccessful-").replace("-", "_").upper()
                        previous["terminal"] = timestamp
                # The transaction field is a resource allocation attempt, not
                # an execution-try identity: it can repeat across many retries.
                # READY event order is the stable execution-attempt sequence
                # and matches the debug-derived TaskInfo key.
                attempt = previous_attempt + 1
                current_attempt[task_id] = attempt
                fact = attempts.setdefault((task_id, attempt), {"task_id": task_id, "attempt": attempt})
                fact.update({"category": category, "ready": timestamp, "resource_attempt": resource_attempt})
                continue
            attempt = current_attempt.get(task_id)
            if not attempt:
                # Library processes may transition without a TASK READY record.
                # They are outside application metrics and are not malformed
                # application evidence.
                continue
            fact = attempts.setdefault((task_id, attempt), {"task_id": task_id, "attempt": attempt, "category": ""})
            if state == "RUNNING":
                fact["running"] = timestamp
            elif state == "WAITING_RETRIEVAL":
                fact["waiting_retrieval"] = timestamp
            elif state == "RETRIEVED":
                fact["retrieved"] = timestamp
                fact["terminal"] = timestamp
                fact["outcome"] = fields[2] if len(fields) > 2 else "UNKNOWN"
                resources = _last_json(record.payload)
                fact["worker_start"] = _resource_value(resources, "time_worker_start")
                fact["worker_end"] = _resource_value(resources, "time_worker_end")
            elif state == "DONE":
                fact["done"] = timestamp
                fact.setdefault("outcome", fields[2] if len(fields) > 2 else "UNKNOWN")

        if malformed_payloads:
            self.partial_reasons.append("metric_transaction_payload_malformed")
        all_timestamps = [record.timestamp_us / 1e6 for record in records if record.timestamp_us is not None]
        if not all_timestamps:
            self._write_empty_transaction_outputs()
            return None
        start_epoch = starts[-1] if starts else min(all_timestamps)
        end_epoch = ends[-1] if ends else max(all_timestamps)
        run_complete = bool(ends)
        transaction_status = "PASS" if not malformed_payloads and not self.provenance["transactions"]["malformed_records"] else "PARTIAL"
        source_status = transaction_status
        transaction_reasons = []
        if transaction_status != "PASS":
            transaction_reasons.append("Malformed transaction evidence prevents complete validation.")
        if not run_complete:
            self.partial_reasons.append("metric_manager_end_missing")
        observed_scope_caveat = (
            "Run has no MANAGER END; values cover observations through the latest source record."
            if not run_complete else ""
        )

        app_attempts = []
        terminal_events = []
        failure_events = []
        latency_rows = []
        terminal_application = 0
        complete_queue = complete_execution = complete_retrieval = 0
        classified_attempts = 0
        for key in sorted(attempts):
            fact = attempts[key]
            outcome = fact.get("outcome")
            is_library, is_recovery, classified = self._task_classification(fact["task_id"], fact["attempt"], outcome)
            classified_attempts += classified
            if is_library or is_recovery:
                continue
            app_attempts.append(fact)
            terminal_time = fact.get("terminal") or fact.get("retrieved") or fact.get("done")
            if terminal_time is not None and outcome:
                terminal_application += 1
                if outcome == "SUCCESS":
                    terminal_events.append((terminal_time, {"successful": 1.0, "failed": 0.0}))
                else:
                    terminal_events.append((terminal_time, {"successful": 0.0, "failed": 1.0}))
                    failure_events.append((terminal_time, outcome))
            queue = None
            if fact.get("ready") is not None and fact.get("running") is not None:
                queue = max(0.0, fact["running"] - fact["ready"])
                complete_queue += 1
            execution = None
            if fact.get("worker_start") is not None and fact.get("worker_end") is not None:
                execution = max(0.0, fact["worker_end"] - fact["worker_start"])
                complete_execution += 1
            elif fact.get("running") is not None and fact.get("waiting_retrieval") is not None:
                execution = max(0.0, fact["waiting_retrieval"] - fact["running"])
                complete_execution += 1
            elif fact.get("running") is not None and fact.get("terminal") is not None:
                execution = max(0.0, fact["terminal"] - fact["running"])
                complete_execution += 1
            retrieval = None
            if fact.get("waiting_retrieval") is not None and fact.get("retrieved") is not None:
                retrieval = max(0.0, fact["retrieved"] - fact["waiting_retrieval"])
                complete_retrieval += 1
            latency_rows.append({
                "task_id": fact["task_id"], "attempt": fact["attempt"],
                "category": fact.get("category", ""), "outcome": outcome or "INCOMPLETE",
                "queue_seconds": queue, "execution_seconds": execution,
                "retrieval_seconds": retrieval,
            })

        throughput_rows = _bucket_rows(terminal_events, start_epoch, end_epoch, ["successful", "failed"])
        for row in throughput_rows:
            row["successful"] = int(row["successful"])
            row["failed"] = int(row["failed"])
            row["success_rate_per_second"] = row["successful"] / row["bucket_seconds"]
            row["failure_rate_per_second"] = row["failed"] / row["bucket_seconds"]
        _write_csv(self.csv_dir / "metric_task_throughput.csv", ["time", "bucket_seconds", "successful", "failed", "success_rate_per_second", "failure_rate_per_second"], throughput_rows)
        _write_csv(self.csv_dir / "metric_task_latency.csv", ["task_id", "attempt", "category", "outcome", "queue_seconds", "execution_seconds", "retrieval_seconds"], latency_rows)

        failure_buckets = defaultdict(Counter)
        width = max(1.0, math.ceil(max(0.0, end_epoch - start_epoch) / 400))
        failure_bucket_count = max(1, math.ceil(max(0.0, end_epoch - start_epoch) / width))
        for timestamp, status in failure_events:
            index = min(failure_bucket_count - 1, int(max(0, timestamp - start_epoch) // width))
            failure_buckets[index][status] += 1
        failure_statuses = sorted({status for counts in failure_buckets.values() for status in counts})
        failure_rows = [
            {"time": index * width, "status": status, "count": failure_buckets[index][status]}
            for index in range(failure_bucket_count)
            for status in failure_statuses
        ]
        _write_csv(self.csv_dir / "metric_failure_timeline.csv", ["time", "status", "count"], failure_rows)

        churn_rows = _worker_event_rows(worker_events, start_epoch, end_epoch)
        _write_csv(
            self.csv_dir / "metric_worker_churn.csv",
            ["time", "bucket_seconds", "reason", "count", "events_per_second"], churn_rows,
        )

        transfer_bucket_events = []
        total_input = total_output = 0.0
        for timestamp, direction, size_bytes, _ in transfer_events:
            values = {"input_bytes": 0.0, "output_bytes": 0.0}
            if direction == "INPUT":
                values["input_bytes"] = size_bytes
                total_input += size_bytes
            elif direction == "OUTPUT":
                values["output_bytes"] = size_bytes
                total_output += size_bytes
            transfer_bucket_events.append((timestamp, values))
        transfer_rows = _bucket_rows(transfer_bucket_events, start_epoch, end_epoch, ["input_bytes", "output_bytes"])
        for row in transfer_rows:
            row["input_bytes_per_second"] = row["input_bytes"] / row["bucket_seconds"]
            row["output_bytes_per_second"] = row["output_bytes"] / row["bucket_seconds"]
        _write_csv(self.csv_dir / "metric_transfer_throughput.csv", ["time", "bucket_seconds", "input_bytes", "output_bytes", "input_bytes_per_second", "output_bytes_per_second"], transfer_rows)

        total_attempts = len(app_attempts)
        # A run without task attempts has nothing left unclassified.
        classification_coverage = classified_attempts / len(attempts) if attempts else 1.0
        if classification_coverage < 0.995:
            self.partial_reasons.append("metric_task_classification_incomplete")
            source_status = "PARTIAL"
        classification_reason = (
            f"Only {classification_coverage:.1%} of task attempts have debug classification evidence."
            if classification_coverage < 0.995 else ""
        )
        source_reasons = transaction_reasons + ([classification_reason] if classification_reason else [])
        terminal_coverage = terminal_application / total_attempts if total_attempts else 0
        latency_denominator = max(1, total_attempts)
        status = source_status if terminal_coverage >= 0.995 else "PARTIAL"
        operational_status = transaction_status if classification_coverage >= 0.995 else "PARTIAL"
        operational_reasons = transaction_reasons + ([classification_reason] if classification_reason else [])
        terminal_reasons = list(source_reasons)
        if terminal_coverage < 0.995:
            terminal_reasons.append(
                f"Only {terminal_coverage:.1%} of application attempts have terminal evidence."
            )
        workflow_rows, workflow_observed = _workflow_pressure_rows(app_attempts, start_epoch, end_epoch)
        _write_csv(
            self.csv_dir / "metric_workflow_pressure.csv",
            ["time", "queued", "executing", "retrieving"], workflow_rows,
        )
        _write_csv(
            self.csv_dir / "metric_retry_pressure.csv",
            ["time", "bucket_seconds", "first_attempts", "retries", "first_attempt_rate_per_second", "retry_rate_per_second"],
            _retry_pressure_rows(app_attempts, start_epoch, end_epoch),
        )
        workflow_coverage = workflow_observed / max(1, len(app_attempts))
        open_attempts = sum(
            fact.get("terminal") is None and fact.get("done") is None
            for fact in app_attempts
        )
        workflow_caveat = (
            f"{open_attempts} open attempts are right-censored at the latest observation."
            if open_attempts else ""
        )
        retry_caveat = "Future attempts are not yet observed." if not run_complete else ""
        self._contract(
            "workflow-pressure", operational_status, "transactions+debug-classification",
            min(workflow_coverage, classification_coverage), "attempts",
            "observed application attempt lifecycle states", "library,recovery", workflow_caveat,
            "; ".join(operational_reasons),
        )
        self._contract(
            "retry-pressure", operational_status, "transactions+debug-classification",
            classification_coverage, "attempts/s", "application READY observations",
            "library,recovery", retry_caveat, "; ".join(operational_reasons),
        )
        throughput_caveat = "; ".join(filter(None, (
            "Terminal event is RETRIEVED; no synthetic completions.",
            observed_scope_caveat,
        )))
        self._contract(
            "task-throughput", status, "transactions+debug-classification",
            min(terminal_coverage, classification_coverage), "attempts/s",
            "application attempts only", "library,recovery,incomplete",
            throughput_caveat,
            "; ".join(terminal_reasons),
        )
        latency_coverage = min(complete_queue, complete_execution) / latency_denominator
        latency_reasons = list(source_reasons)
        if latency_coverage < 0.995:
            latency_reasons.append(
                f"Only {latency_coverage:.1%} of application attempts have complete queue and execution endpoints."
            )
        latency_caveat = "; ".join(filter(None, (
            "Each latency field has independent coverage.", observed_scope_caveat,
        )))
        self._contract(
            "task-latency", source_status if latency_coverage >= 0.995 else "PARTIAL",
            "transactions+debug-classification",
            min(latency_coverage, classification_coverage), "seconds",
            "application attempts only", "missing phase endpoints",
            latency_caveat,
            "; ".join(latency_reasons),
        )
        failure_caveat = "; ".join(filter(None, (
            "Statuses are TaskVine terminal outcomes.", observed_scope_caveat,
        )))
        self._contract(
            "failure-timeline", status, "transactions+debug-classification",
            min(terminal_coverage, classification_coverage), "attempts",
            "unsuccessful application attempts", "library,recovery,incomplete",
            failure_caveat,
            "; ".join(terminal_reasons),
        )
        self._contract(
            "worker-churn", transaction_status, "transactions", 1.0,
            "events/s", "observed worker connection and disconnection events", "none",
            "Disconnect reasons are source values; open sessions require no synthetic disconnect.",
            "; ".join(transaction_reasons),
        )
        transfer_caveat = "; ".join(filter(None, (
            "Payload throughput is not physical link utilization.", observed_scope_caveat,
        )))
        self._contract(
            "transfer-throughput", source_status, "transactions",
            1.0 if transfer_events else 0.0, "bytes/s",
            "completed INPUT and OUTPUT payloads", "avoided/cache-hit transfers",
            transfer_caveat, "; ".join(transaction_reasons),
        )
        self._transaction_totals = {"input_bytes": total_input, "output_bytes": total_output, "worker_connections": sum(delta > 0 for _, _, delta, _ in worker_events)}
        return start_epoch

    def _build_capacity(self, rows, start_epoch):
        fields = ["time", "active_workers", "tasks_running", "total_cores", "committed_cores", "core_occupancy_ratio"]
        if not rows:
            _write_csv(self.csv_dir / "metric_worker_capacity.csv", fields, [])
            reason = "Performance source file has no usable samples."
            self._contract(
                "worker-capacity", "UNAVAILABLE", "performance", 0, "cores",
                "manager capacity samples", "all", status_reason=reason,
            )
            self._contract(
                "core-occupancy", "UNAVAILABLE", "performance", 0, "ratio",
                "manager capacity samples", "all", status_reason=reason,
            )
            return
        if start_epoch is None:
            start_epoch = rows[0]["timestamp"] / 1e6
        output = []
        valid = 0
        for row in rows:
            total = row.get("total_cores", 0.0)
            committed = row.get("committed_cores", 0.0)
            ratio = committed / total if total > 0 else None
            valid += all(
                key in row
                for key in ("timestamp", "workers_connected", "tasks_running", "total_cores", "committed_cores")
            )
            output.append({
                "time": max(0.0, row["timestamp"] / 1e6 - start_epoch),
                "active_workers": row.get("workers_connected"),
                "tasks_running": row.get("tasks_running"),
                "total_cores": total,
                "committed_cores": committed,
                "core_occupancy_ratio": ratio,
            })
        # Preserve endpoints and extrema while bounding presentation size.
        if len(output) > 4000:
            stride = math.ceil(len(output) / 3996)
            selected = {0, len(output) - 1}
            selected.update(range(0, len(output), stride))
            for key in ("active_workers", "tasks_running", "total_cores", "committed_cores"):
                selected.add(max(range(len(output)), key=lambda index: output[index][key] or 0))
            output = [output[index] for index in sorted(selected)]
        _write_csv(self.csv_dir / "metric_worker_capacity.csv", fields, output)

        transfer_ok = True
        churn_ok = True
        transfer_caveats = []
        churn_caveats = []
        totals = getattr(self, "_transaction_totals", None)
        if totals:
            last = rows[-1]
            for perf_key, tx_key in (("bytes_sent", "input_bytes"), ("bytes_received", "output_bytes")):
                if perf_key not in last:
                    transfer_ok = False
                    transfer_caveats.append(f"{perf_key}_missing")
                    continue
                observed = float(last.get(perf_key, 0.0))
                expected = float(totals[tx_key])
                tolerance = max(1.0, abs(observed) * 0.01)
                if abs(observed - expected) > tolerance:
                    transfer_ok = False
                    transfer_caveats.append(f"{perf_key}_mismatch")
            if "workers_joined" not in last:
                churn_ok = False
                churn_caveats.append("workers_joined_missing")
            elif int(last["workers_joined"]) != totals["worker_connections"]:
                churn_ok = False
                churn_caveats.append("worker_connections_mismatch")
        if not transfer_ok or not churn_ok:
            self.partial_reasons.append("metric_cross_source_mismatch")
        if not transfer_ok:
            self._downgrade_contract(
                "transfer-throughput",
                f"Transaction/performance byte conservation failed: {', '.join(transfer_caveats)}.",
            )
        if not churn_ok:
            self._downgrade_contract(
                "worker-churn",
                f"Worker connection conservation failed: {', '.join(churn_caveats)}.",
            )
        coverage = valid / len(rows)
        performance_malformed = self.provenance["performance"]["malformed_records"]
        status = "PASS" if coverage >= 0.995 and not performance_malformed else "PARTIAL"
        capacity_reasons = []
        if coverage < 0.995:
            capacity_reasons.append(f"Only {coverage:.1%} of performance samples have required fields.")
        if performance_malformed:
            capacity_reasons.append("Malformed performance records prevent complete validation.")
        capacity_reason = "; ".join(capacity_reasons)
        self._contract(
            "worker-capacity", status, "performance", coverage, "cores",
            "manager capacity samples", "zero-capacity samples from occupancy ratio",
            "Committed capacity is a scheduler occupancy proxy, not CPU utilization.",
            capacity_reason,
        )
        self._contract(
            "core-occupancy", status, "performance", coverage, "ratio",
            "committed cores divided by total cores", "zero-capacity samples",
            "Scheduler occupancy proxy; not measured CPU utilization.", capacity_reason,
        )


def generate_metric_facts(runtime_template: str, data_parser):
    return MetricFactBuilder(runtime_template, data_parser).build()
