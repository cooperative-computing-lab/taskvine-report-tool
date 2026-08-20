"""Read source-backed metrics from an immutable report snapshot."""

from __future__ import annotations

import csv
import copy
import math
from functools import lru_cache
from pathlib import Path

from src.vine_parse.metric_facts import METRIC_SEMANTICS_VERSION


METRICS = {
    "task-throughput": {
        "file": "metric_task_throughput.csv", "x": "time",
        "series": (("Successful", "success_rate_per_second"), ("Failed", "failure_rate_per_second")),
        "x_label": "Runtime", "y_label": "Attempts / second", "kind": "line",
    },
    "task-latency": {
        "file": "metric_task_latency.csv", "kind": "cdf",
        "series": (("Queue", "queue_seconds"), ("Execution", "execution_seconds"), ("Retrieval", "retrieval_seconds")),
        "x_label": "Duration", "y_label": "Percentile",
    },
    "failure-timeline": {
        "file": "metric_failure_timeline.csv", "kind": "grouped",
        "x": "time", "group": "status", "value": "count",
        "x_label": "Runtime", "y_label": "Failed attempts",
    },
    "worker-capacity": {
        "file": "metric_worker_capacity.csv", "x": "time", "kind": "line",
        "series": (("Total cores", "total_cores"), ("Committed cores", "committed_cores"), ("Running tasks", "tasks_running")),
        "x_label": "Runtime", "y_label": "Count",
    },
    "worker-churn": {
        "file": "metric_worker_churn.csv", "x": "time", "kind": "line",
        "series": (("Active workers", "active"),),
        "x_label": "Runtime", "y_label": "Workers",
    },
    "transfer-throughput": {
        "file": "metric_transfer_throughput.csv", "x": "time", "kind": "line",
        "series": (("Input", "input_bytes_per_second"), ("Output", "output_bytes_per_second")),
        "x_label": "Runtime", "y_label": "Payload bytes / second",
    },
}


def _number(value):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _contracts(csv_dir: Path):
    path = csv_dir / "metric_contracts.csv"
    if not path.is_file():
        return {}
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        return {row.get("metric_id"): row for row in csv.DictReader(handle)}


def _bounded(points, maximum=1000):
    if len(points) <= maximum:
        return points
    stride = math.ceil(len(points) / maximum)
    selected = set(range(0, len(points), stride))
    selected.update((0, len(points) - 1))
    finite_y = [(index, _number(point[1])) for index, point in enumerate(points)]
    finite_y = [(index, value) for index, value in finite_y if value is not None]
    if finite_y:
        selected.add(min(finite_y, key=lambda item: item[1])[0])
        selected.add(max(finite_y, key=lambda item: item[1])[0])
    return [points[index] for index in sorted(selected)]


def _cdf(values):
    ordered = sorted(value for value in values if value is not None)
    if not ordered:
        return []
    points = [[value, (index + 1) / len(ordered)] for index, value in enumerate(ordered)]
    return _bounded(points)


@lru_cache(maxsize=128)
def _metric_payload(csv_dir_string: str, metric_id: str):
    definition = METRICS.get(metric_id)
    if definition is None:
        return None
    csv_dir = Path(csv_dir_string)
    contract = _contracts(csv_dir).get(metric_id)
    if contract is None:
        contract = {
            "metric_id": metric_id,
            "semantics_version": str(METRIC_SEMANTICS_VERSION),
            "status": "UNAVAILABLE",
            "source": "",
            "coverage": "0",
            "unit": "",
            "inclusion": "",
            "exclusions": "all",
            "caveat": "Run was parsed before source-backed metrics were generated.",
        }
    normalized_contract = {
        **contract,
        "semantics_version": int(contract.get("semantics_version") or METRIC_SEMANTICS_VERSION),
        "coverage": _number(contract.get("coverage")) or 0.0,
    }
    available = normalized_contract["status"] == "PASS"
    payload = {
        "metric_id": metric_id,
        "available": available,
        "contract": normalized_contract,
        "kind": definition["kind"],
        "x_label": definition["x_label"],
        "y_label": definition["y_label"],
        "series": [],
    }
    # Fail closed: PARTIAL/UNKNOWN facts remain inspectable through the
    # contract, but are never sent as plot-ready points.
    if not available:
        return payload
    path = csv_dir / definition["file"]
    if not path.is_file():
        payload["available"] = False
        payload["contract"]["status"] = "UNAVAILABLE"
        payload["contract"]["caveat"] = "Committed metric data is missing."
        return payload
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        rows = list(csv.DictReader(handle))
    if definition["kind"] == "cdf":
        for label, column in definition["series"]:
            payload["series"].append({
                "name": label,
                "points": _cdf([_number(row.get(column)) for row in rows]),
            })
    elif definition["kind"] == "grouped":
        grouped = {}
        for row in rows:
            group = row.get(definition["group"]) or "Unknown"
            x = _number(row.get(definition["x"]))
            y = _number(row.get(definition["value"]))
            if x is not None and y is not None:
                grouped.setdefault(group, []).append([x, y])
        payload["series"] = [
            {"name": name, "points": _bounded(sorted(points))}
            for name, points in sorted(grouped.items())
        ]
    else:
        for label, column in definition["series"]:
            points = []
            for row in rows:
                x = _number(row.get(definition["x"]))
                y = _number(row.get(column))
                if x is not None and y is not None:
                    points.append([x, y])
            payload["series"].append({"name": label, "points": _bounded(points)})
    payload["available"] = any(series["points"] for series in payload["series"])
    return payload


def metric_payload(snapshot, metric_id: str):
    """Return a safe copy of a payload cached by immutable snapshot path."""
    payload = _metric_payload(str(snapshot.csv_files_dir), metric_id)
    return copy.deepcopy(payload)
