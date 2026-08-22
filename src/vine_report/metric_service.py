"""Read source-backed metrics from an immutable report snapshot."""

from __future__ import annotations

import csv
import copy
import math
from functools import lru_cache
from pathlib import Path

from src.vine_parse.metric_facts import METRIC_SEMANTICS_VERSION
from src.vine_report.metric_registry import metric_spec
from src.vine_report.temporal_series import normalize_temporal_points, time_context


def _definition(metric_id):
    spec = metric_spec(metric_id)
    if spec is None or spec.mode != "source":
        return None
    options = spec.options
    definition = {
        "file": spec.artifact,
        "x": spec.x_field,
        "kind": spec.renderer,
        "series": spec.series_columns,
        "x_label": options.get("x_label", spec.x_unit),
        "y_label": options.get("y_label", spec.y_unit),
    }
    if spec.renderer == "grouped":
        definition.update({"group": options["group"], "value": options["value"]})
    return definition


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
    definition = _definition(metric_id)
    if definition is None:
        return None
    csv_dir = Path(csv_dir_string)
    spec = metric_spec(metric_id)
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
    context = time_context(csv_dir_string)
    if spec.time_linked:
        bucket_width = next(
            (_number(row.get("bucket_seconds")) for row in rows if _number(row.get("bucket_seconds")) is not None),
            None,
        )
        for series in payload["series"]:
            series["points"] = normalize_temporal_points(
                series["points"], context,
                origin=spec.time_origin,
                policy=spec.temporal_policy,
                bucket_width=bucket_width,
            )
        if context.domain is not None:
            payload["axes"] = {
                "x": {"unit": spec.x_unit, "domain": list(context.domain), "label": definition["x_label"]},
                "y": {"unit": spec.y_unit, "label": definition["y_label"]},
            }
    payload["available"] = any(series["points"] for series in payload["series"])
    return payload


def metric_payload(snapshot, metric_id: str):
    """Return a safe copy of a payload cached by immutable snapshot path."""
    payload = _metric_payload(str(snapshot.csv_files_dir), metric_id)
    return copy.deepcopy(payload)
