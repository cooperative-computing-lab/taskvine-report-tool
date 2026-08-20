"""Read-only product dashboard APIs built from generated report CSV files."""

from __future__ import annotations

import csv
import json
import math
from functools import lru_cache
from pathlib import Path

from flask import Blueprint, current_app, g, jsonify, request
import polars as pl


dashboard_bp = Blueprint("dashboard", __name__, url_prefix="/api/dashboard")
API_VERSION = "1"


class DashboardAPIError(Exception):
    """A stable, client-facing Dashboard API error."""

    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


@dashboard_bp.errorhandler(DashboardAPIError)
def _handle_api_error(error: DashboardAPIError):
    return jsonify({"error": {"code": error.code, "message": error.message}}), error.status


@dashboard_bp.after_request
def _dashboard_headers(response):
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-TaskVine-API-Version"] = API_VERSION
    return response


def _run_dir(folder: str) -> Path:
    bound = getattr(g, "run_snapshot", None)
    if bound is not None and bound.run_id == folder:
        return Path(bound.runtime_template)
    try:
        return Path(current_app.config["SNAPSHOT_STORE"].refresh(folder).runtime_template)
    except Exception as error:
        from src.vine_report.run_store import RunStoreError
        if isinstance(error, RunStoreError):
            raise DashboardAPIError(error.code, error.message, error.status) from error
        raise


def _runs() -> list[str]:
    return current_app.config["RUN_CATALOG"].list_runs()


def _coerce(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        try:
            number = float(value)
            return int(number) if number.is_integer() else number
        except ValueError:
            return value


def _metadata(csv_dir: Path) -> dict:
    result = {}
    with (csv_dir / "metadata.csv").open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            result[row["key"]] = _coerce(row["value"])
    return result


def _numeric_values(path: Path, column: str) -> list[float]:
    if not path.is_file():
        return []
    values = []
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        for row in csv.DictReader(handle):
            try:
                value = float(row.get(column, ""))
            except (TypeError, ValueError):
                continue
            if math.isfinite(value):
                values.append(value)
    return values


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    low = math.floor(index)
    high = math.ceil(index)
    if low == high:
        return ordered[low]
    return ordered[low] * (high - index) + ordered[high] * (index - low)


def _series_stats(path: Path, column: str) -> dict:
    values = _numeric_values(path, column)
    if not values:
        return {"mean": None, "p50": None, "p95": None, "max": None}
    return {
        "mean": sum(values) / len(values),
        "p50": _percentile(values, 0.5),
        "p95": _percentile(values, 0.95),
        "max": max(values),
    }


def _max_series_value(path: Path, column: str) -> float | None:
    values = _numeric_values(path, column)
    return max(values) if values else None


def _peak_wide_total(path: Path, ignored_columns: int = 1) -> float | None:
    if not path.is_file():
        return None
    # These matrices can contain thousands of sparse worker columns. Polars
    # keeps the reduction fast; full schema inference is required because a
    # worker column may remain empty for thousands of initial rows.
    frame = pl.read_csv(path, infer_schema_length=None, ignore_errors=True)
    if frame.width <= ignored_columns or frame.height == 0:
        return None
    value = frame.select(
        pl.sum_horizontal(pl.exclude(frame.columns[:ignored_columns])).max()
    ).item()
    return float(value) if value is not None else None


def _summary_stamp(csv_dir: Path) -> tuple:
    names = (
        "metadata.csv", "task_execution_time.csv", "task_response_time.csv",
        "worker_concurrency.csv", "worker_lifetime.csv",
        "file_transferred_size.csv", "file_created_size.csv",
        "worker_storage_consumption.csv",
    )
    csv_stamp = tuple(
        (name, (csv_dir / name).stat().st_mtime_ns if (csv_dir / name).exists() else 0)
        for name in names
    )
    diagnostics = csv_dir.parent / "vine-logs" / "parse-diagnostics.json"
    manifest = csv_dir / "parse-manifest.json"
    return csv_stamp + (
        ("parse-diagnostics.json", diagnostics.stat().st_mtime_ns if diagnostics.exists() else 0),
        ("parse-manifest.json", manifest.stat().st_mtime_ns if manifest.exists() else 0),
    )


def _data_quality(run_dir: Path) -> dict:
    diagnostics_path = run_dir / "vine-logs" / "parse-diagnostics.json"
    manifest_path = run_dir / "csv-files" / "parse-manifest.json"
    if not diagnostics_path.is_file():
        return {
            "status": "UNKNOWN",
            "diagnostics_available": False,
            "manifest_available": manifest_path.is_file(),
            "reasons": ["parse_diagnostics_missing"],
        }
    try:
        with diagnostics_path.open(encoding="utf-8") as handle:
            diagnostics = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {
            "status": "UNKNOWN",
            "diagnostics_available": False,
            "manifest_available": manifest_path.is_file(),
            "reasons": ["parse_diagnostics_unreadable"],
        }

    source = diagnostics.get("source", {})
    compatibility = diagnostics.get("compatibility", {})
    return {
        "status": diagnostics.get("status", "UNKNOWN"),
        "diagnostics_available": True,
        "manifest_available": manifest_path.is_file(),
        "parser_contract_version": diagnostics.get("parser_contract_version"),
        "source_format": source.get("format"),
        "source_adapter": source.get("adapter"),
        "source_sha256": source.get("sha256"),
        "format_fingerprint_sha256": source.get("format_fingerprint_sha256"),
        "handled_lines": source.get("handled_lines"),
        "unhandled_lines": source.get("unhandled_lines"),
        "protocol_candidate_unhandled_lines": source.get("protocol_candidate_unhandled_lines"),
        "timezone_evidence": compatibility.get("timezone"),
        "reasons": diagnostics.get("partial_reasons", []),
    }


def _finding(
    severity: str,
    title: str,
    detail: str,
    metric: str,
    observed: float | None = None,
    operator: str | None = None,
    threshold: float | None = None,
):
    evidence = {"metric": metric, "observed": observed}
    if operator is not None:
        evidence.update({"operator": operator, "threshold": threshold})
    return {
        "severity": severity,
        "title": title,
        "detail": detail,
        "evidence": evidence,
    }


def _build_findings(summary: dict) -> list[dict]:
    findings = []
    rate = summary["success_rate"]
    if rate is not None and rate < 0.8:
        findings.append(_finding(
            "critical", "Low task success rate",
            f"Only {rate * 100:.1f}% of application tasks completed successfully.",
            "success_rate", rate, "lt", 0.8,
        ))
    elif rate is not None and rate < 0.98:
        findings.append(_finding(
            "warning", "Task failures need review",
            f"The run completed with a {100 - rate * 100:.1f}% unsuccessful-task rate.",
            "success_rate", rate, "lt", 0.98,
        ))
    elif rate is not None:
        findings.append(_finding(
            "positive", "Task completion is healthy",
            f"{rate * 100:.1f}% of application tasks completed successfully.",
            "success_rate", rate, "gte", 0.98,
        ))

    execution = summary["timing"]["execution"]
    response = summary["timing"]["response"]
    if execution["p50"] and execution["p95"] and execution["p95"] > execution["p50"] * 5:
        findings.append(_finding(
            "warning", "Long-tail execution detected",
            f"P95 execution time is {execution['p95']:.1f}s, more than 5x the median.",
            "execution_p95_to_p50_ratio", execution["p95"] / execution["p50"], "gt", 5,
        ))
    if response["mean"] and execution["mean"] and response["mean"] > execution["mean"]:
        findings.append(_finding(
            "warning", "Scheduling latency dominates",
            f"Mean response time ({response['mean']:.1f}s) exceeds mean execution time ({execution['mean']:.1f}s).",
            "response_to_execution_ratio", response["mean"] / execution["mean"], "gt", 1,
        ))

    recovery = summary["recovery"]
    if recovery["total"]:
        recovery_rate = recovery["successful"] / recovery["total"]
        severity = "positive" if recovery_rate >= 0.95 else "warning"
        findings.append(_finding(
            severity, "Recovery activity observed",
            f"{recovery['successful']:,} of {recovery['total']:,} recovery tasks succeeded ({recovery_rate * 100:.1f}%).",
            "recovery_rate", recovery_rate, "gte" if severity == "positive" else "lt", 0.95,
        ))

    if not findings:
        findings.append(_finding(
            "neutral", "Run data is available",
            "No rule-based anomaly crossed the current dashboard thresholds.",
            "data_available",
        ))
    return findings[:4]


@lru_cache(maxsize=64)
def _cached_summary(csv_dir_string: str, stamp: tuple, run_id: str) -> dict:
    del stamp
    csv_dir = Path(csv_dir_string)
    meta = _metadata(csv_dir)
    total = int(meta.get("total_tasks") or 0)
    successful = int(meta.get("successful_tasks") or 0)
    unsuccessful = int(meta.get("unsuccessful_tasks") or 0)
    duration = float(meta.get("manager_duration") or 0)

    summary = {
        "run": run_id,
        "data_quality": _data_quality(csv_dir.parent),
        "start_time": meta.get("manager_start_time"),
        "end_time": meta.get("manager_end_time"),
        "duration_seconds": duration,
        "total_tasks": total,
        "total_all_tasks": int(meta.get("total_all_tasks") or total),
        "library_tasks": int(meta.get("total_library_tasks") or 0),
        "successful_tasks": successful,
        "unsuccessful_tasks": unsuccessful,
        "success_rate": successful / total if total else None,
        "workers": int(meta.get("total_workers") or 0),
        "files": int(meta.get("total_files") or 0),
        "throughput_tasks_per_second": total / duration if duration else None,
        "recovery": {
            "total": int(meta.get("recovery_tasks") or 0),
            "successful": int(meta.get("recovery_successful") or 0),
            "unsuccessful": int(meta.get("recovery_unsuccessful") or 0),
        },
        "timing": {
            "execution": _series_stats(csv_dir / "task_execution_time.csv", "Execution Time"),
            "response": _series_stats(csv_dir / "task_response_time.csv", "Response Time"),
            "worker_lifetime": _series_stats(csv_dir / "worker_lifetime.csv", "LifeTime (s)"),
        },
        "peak_active_workers": _max_series_value(
            csv_dir / "worker_concurrency.csv", "Active Workers (count)"
        ),
        "transferred_mb": _max_series_value(
            csv_dir / "file_transferred_size.csv", "cumulative_size_mb"
        ),
        "created_mb": _max_series_value(
            csv_dir / "file_created_size.csv", "cumulative_size_mb"
        ),
        "peak_storage_mb": _peak_wide_total(
            csv_dir / "worker_storage_consumption.csv", ignored_columns=2
        ),
    }
    summary["findings"] = _build_findings(summary)
    return summary


def _summary(folder: str) -> dict:
    csv_dir = _run_dir(folder) / "csv-files"
    try:
        return _cached_summary(str(csv_dir), _summary_stamp(csv_dir), folder)
    except DashboardAPIError:
        raise
    except (OSError, csv.Error, KeyError, TypeError, ValueError, pl.exceptions.PolarsError) as error:
        raise DashboardAPIError(
            "run_data_invalid", "run data could not be read", 422
        ) from error


@dashboard_bp.route("/runs")
def runs():
    return jsonify({"runs": _runs()})


@dashboard_bp.route("/summary")
def summary():
    bound = getattr(g, "run_snapshot", None)
    folder = request.args.get("folder") or (bound.run_id if bound else "")
    return jsonify(_summary(folder))


@dashboard_bp.route("/compare")
def compare():
    requested = [item.strip() for item in request.args.get("folders", "").split(",") if item.strip()]
    folders = list(dict.fromkeys(requested))
    return jsonify(_comparison(folders))


def _comparison(folders: list[str]) -> dict:
    if len(folders) < 2:
        raise DashboardAPIError("invalid_parameter", "at least two runs are required")
    if len(folders) > 4:
        raise DashboardAPIError("invalid_parameter", "at most four runs can be compared")
    return _comparison_from_summaries([_summary(folder) for folder in folders])


def _comparison_from_summaries(summaries: list[dict]) -> dict:
    """Compare already-resolved summaries without reopening mutable run data."""
    baseline = summaries[0]
    keys = (
        "duration_seconds", "success_rate", "total_tasks", "workers",
        "throughput_tasks_per_second", "transferred_mb", "peak_storage_mb",
    )
    rows = []
    for item in summaries:
        deltas = {}
        for key in keys:
            base_value = baseline.get(key)
            value = item.get(key)
            deltas[key] = (
                (value - base_value) / abs(base_value)
                if value is not None and base_value not in (None, 0) else None
            )
        rows.append({"summary": item, "delta_from_baseline": deltas})
    return {"baseline": baseline["run"], "runs": rows}


def _task_value(row: dict, key: str, cast=float):
    raw = row.get(key)
    if raw in (None, ""):
        return None
    try:
        return cast(float(raw)) if cast is int else cast(raw)
    except (TypeError, ValueError):
        return None


@dashboard_bp.route("/tasks")
def tasks():
    bound = getattr(g, "run_snapshot", None)
    folder = request.args.get("folder") or (bound.run_id if bound else "")
    run_dir = _run_dir(folder)
    query = request.args.get("query", "").strip().lower()
    if len(query) > 200:
        raise DashboardAPIError("invalid_parameter", "query must be at most 200 characters")
    status = request.args.get("status", "all")
    if status not in {"all", "successful", "unsuccessful"}:
        raise DashboardAPIError("invalid_parameter", "status is not supported")
    try:
        limit = min(max(int(request.args.get("limit", 50)), 1), 200)
        offset = max(int(request.args.get("offset", 0)), 0)
    except ValueError as error:
        raise DashboardAPIError("invalid_parameter", "limit and offset must be integers") from error
    rows = []
    path = run_dir / "csv-files" / "task_execution_details.csv"
    if not path.is_file():
        raise DashboardAPIError("run_data_unavailable", "task details are unavailable", 422)
    try:
        with path.open(newline="", encoding="utf-8", errors="replace") as handle:
            for row in csv.DictReader(handle):
                record_type = row.get("record_type", "")
                if record_type not in ("successful_tasks", "unsuccessful_tasks"):
                    continue
                is_success = record_type == "successful_tasks"
                if status == "successful" and not is_success:
                    continue
                if status == "unsuccessful" and is_success:
                    continue
                searchable = " ".join((
                    row.get("task_id", ""), row.get("category", ""),
                    row.get("worker_entry", ""), row.get("input_files", ""),
                    row.get("output_files", ""),
                )).lower()
                if query and query not in searchable:
                    continue
                rows.append({
                    "task_id": _task_value(row, "task_id", int),
                    "try_id": _task_value(row, "task_try_id", int),
                    "status": "successful" if is_success else "unsuccessful",
                    "status_code": _task_value(row, "task_status", int),
                    "category": row.get("category") or "Uncategorized",
                    "worker": row.get("worker_entry") or None,
                    "cores": _task_value(row, "cores_requested", int),
                    "execution_seconds": _task_value(row, "execution_time"),
                    "ready_at": _task_value(row, "when_ready"),
                    "running_at": _task_value(row, "when_running"),
                    "done_at": _task_value(row, "when_done"),
                    "input_files": row.get("input_files") or "",
                    "output_files": row.get("output_files") or "",
                    "is_recovery": str(row.get("is_recovery_task", "")).lower() == "true",
                })
        rows.sort(key=lambda item: item["execution_seconds"] or -1, reverse=True)
        page = rows[offset:offset + limit]
        return jsonify({
            "tasks": page,
            "matched": len(rows),
            "limit": limit,
            "offset": offset,
            "has_more": offset + len(page) < len(rows),
        })
    except (OSError, csv.Error) as error:
        raise DashboardAPIError("run_data_invalid", "task details could not be read", 422) from error
