"""Generic, revision-safe query service for standard report charts."""

from __future__ import annotations

import copy
import csv
import json
import math
import threading
from collections import defaultdict
from functools import lru_cache
from pathlib import Path

import polars as pl

from .metric_registry import MetricSpec, metric_spec


class ChartQueryError(ValueError):
    pass


ALLOWED_QUERY_FIELDS = {"start", "end", "series", "max_points", "view", "evidence", "accumulated"}


def _finite(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def normalize_query(args, default_max_points=1000) -> dict:
    unknown = sorted(set(args.keys()) - ALLOWED_QUERY_FIELDS)
    if unknown:
        raise ChartQueryError(f"unsupported query parameter(s): {', '.join(unknown)}")
    start, end = _finite(args.get("start")), _finite(args.get("end"))
    if start is not None and start < 0:
        raise ChartQueryError("start must be non-negative")
    if start is not None and end is not None and end <= start:
        raise ChartQueryError("end must be greater than start")
    supplied = args.get("max_points")
    try:
        maximum = int(supplied if supplied is not None else default_max_points)
    except (TypeError, ValueError) as error:
        raise ChartQueryError("max_points must be an integer") from error
    if supplied is None:
        maximum = min(5000, max(10, maximum))
    elif not 10 <= maximum <= 5000:
        raise ChartQueryError("max_points must be between 10 and 5000")
    selected = args.getlist("series") if hasattr(args, "getlist") else args.get("series", [])
    if isinstance(selected, str):
        selected = [selected]
    evidence = args.get("evidence", "all")
    if evidence not in {"all", "observed"}:
        raise ChartQueryError("evidence must be 'all' or 'observed'")
    view = args.get("view")
    accumulated = str(args.get("accumulated", "")).lower()
    if view is None and accumulated in {"true", "false"}:
        view = "total" if accumulated == "true" else "by-worker"
    return {
        "start": start, "end": end, "series": tuple(sorted(set(selected))),
        "max_points": maximum, "view": view, "evidence": evidence,
    }


def _contract(csv_dir: Path, spec: MetricSpec) -> dict:
    path = csv_dir / "metric_contracts.csv"
    accepted = {spec.id}
    if spec.id == "worker-storage-consumption":
        accepted.add("worker-cache-occupancy")
    if path.is_file():
        with path.open(newline="", encoding="utf-8", errors="replace") as handle:
            for row in csv.DictReader(handle):
                if row.get("metric_id") in accepted:
                    return {
                        **row,
                        "semantics_version": int(row.get("semantics_version") or spec.semantic_version),
                        "coverage": _finite(row.get("coverage")),
                    }
    diagnostics = {}
    path = csv_dir.parent / "vine-logs" / "parse-diagnostics.json"
    if path.is_file():
        try:
            diagnostics = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            pass
    return {
        "metric_id": spec.id, "semantics_version": spec.semantic_version,
        "status": diagnostics.get("status") or "UNKNOWN", "source": spec.source,
        "coverage": None, "unit": spec.y_unit, "inclusion": spec.inclusion,
        "exclusions": spec.exclusions,
        "caveat": "Legacy projection has no metric-specific coverage evidence.",
    }


def _extent(series, index):
    values = [
        float(point[index]) for item in series for point in item.get("points", [])
        if len(point) > index and _finite(point[index]) is not None
    ]
    if not values:
        return [0.0, 1.0]
    low, high = min(values), max(values)
    if low == high:
        return [min(0.0, low), high + max(abs(high) * 0.05, 1.0)]
    return [min(0.0, low), high]


def _evenly_add(target, candidates, slots):
    if slots <= 0 or not candidates:
        return
    stride = len(candidates) / slots
    target.update(candidates[min(int(index * stride), len(candidates) - 1)] for index in range(slots))


def _sample_points(points: list[list], maximum: int, state: bool = False) -> list[list]:
    if len(points) <= maximum:
        return points
    finite = [index for index, point in enumerate(points) if len(point) > 1 and _finite(point[1]) is not None]
    mandatory = {0, len(points) - 1}
    if finite:
        mandatory.add(min(finite, key=lambda i: float(points[i][1])))
        mandatory.add(max(finite, key=lambda i: float(points[i][1])))
    selected = set(mandatory)
    if state:
        buckets = max(1, maximum // 4)
        width = len(points) / buckets
        for bucket in range(buckets):
            left = int(bucket * width)
            right = min(len(points), max(left + 1, int((bucket + 1) * width)))
            indices = list(range(left, right))
            selected.update((left, right - 1))
            selected.add(min(indices, key=lambda i: float(points[i][1])))
            selected.add(max(indices, key=lambda i: float(points[i][1])))
    else:
        _evenly_add(selected, [i for i in range(1, len(points) - 1) if i not in selected], maximum - len(selected))
    if len(selected) > maximum:
        retained = set(mandatory)
        _evenly_add(retained, [i for i in sorted(selected) if i not in mandatory], maximum - len(retained))
        selected = retained
    return [points[index] for index in sorted(selected)]


def _window(points, start, end, state=False):
    if start is None and end is None:
        return points
    result, baseline = [], None
    for point in points:
        x = _finite(point[0])
        if x is None:
            continue
        if start is not None and x < start:
            baseline = point
            continue
        if end is not None and x > end:
            break
        result.append(point)
    if state and baseline is not None and start is not None:
        result.insert(0, [start, baseline[1], *baseline[2:]])
    return result


def _read(path: Path) -> pl.DataFrame:
    if not path.is_file():
        raise ChartQueryError(f"metric artifact is missing: {path.name}")
    return pl.read_csv(path, infer_schema_length=None, ignore_errors=False)


def _rank_wide_series(frame, columns, limit):
    if not limit or len(columns) <= limit:
        return columns, {}
    peaks = frame.select([
        pl.col(column).abs().max().fill_null(0).alias(column)
        for column in columns
    ]).row(0, named=True)
    chosen = sorted(columns, key=lambda name: (-float(peaks[name]), name))[:limit]
    return chosen, {
        "available_series": len(columns), "selected_series": len(chosen),
        "omitted_series": len(columns) - len(chosen),
        "series_selection": "highest observed absolute value",
    }


def _build_standard(csv_dir: Path, spec: MetricSpec, query: dict):
    frame = _read(csv_dir / spec.artifact)
    selected = set(query["series"])
    series, lookup, input_points, metadata = [], {}, 0, {}
    state = spec.curve == "step"

    if spec.mode == "wide":
        columns = [name for name in frame.columns if name not in {spec.x_field, *spec.excluded_columns}]
        if selected:
            unknown = selected - set(columns)
            if unknown:
                raise ChartQueryError(f"unknown series: {', '.join(sorted(unknown))}")
            columns = [name for name in columns if name in selected]
        else:
            columns, metadata = _rank_wide_series(
                frame, columns, int(spec.options.get("default_series_limit", 0)),
            )
        for column in columns:
            points = [[float(x), float(y)] for x, y in frame.select([spec.x_field, column]).drop_nulls().iter_rows()]
            points = _window(points, query["start"], query["end"], state)
            input_points += len(points)
            points = _sample_points(points, query["max_points"], state)
            if points:
                series.append({"id": column, "name": column, "points": points})
    elif spec.mode == "columns":
        available = {column for _, column in spec.series_columns}
        unknown = selected - available
        if unknown:
            raise ChartQueryError(f"unknown series: {', '.join(sorted(unknown))}")
        for label, column in spec.series_columns:
            if column not in frame.columns or (selected and column not in selected):
                continue
            points = [[float(x), float(y)] for x, y in frame.select([spec.x_field, column]).drop_nulls().iter_rows()]
            points = _window(points, query["start"], query["end"], state)
            input_points += len(points)
            points = _sample_points(points, query["max_points"], state)
            if points:
                series.append({"id": column, "name": label, "points": points})
    else:
        y_field, scale = spec.y_field, spec.value_scale
        if y_field is None:
            for candidate, candidate_scale in spec.options.get("value_fields", {}).items():
                if candidate in frame.columns:
                    y_field, scale = candidate, float(candidate_scale)
                    break
        if y_field is None:
            raise ChartQueryError("metric artifact has no supported value column")
        required = [spec.x_field, y_field, *spec.extra_fields]
        if spec.lookup_field:
            required.append(spec.lookup_field)
        missing = [column for column in required if column not in frame.columns]
        if missing:
            raise ChartQueryError(f"metric artifact is missing columns: {', '.join(missing)}")
        points = []
        for row in frame.select(required).drop_nulls([spec.x_field, y_field]).iter_rows(named=True):
            x, y = _finite(row[spec.x_field]), _finite(row[y_field])
            if x is None or y is None:
                continue
            point = [x, y * scale, *(row[field] for field in spec.extra_fields)]
            points.append(point)
            if spec.lookup_field:
                lookup[str(x)] = str(row[spec.lookup_field])
        points.sort(key=lambda point: point[0])
        if spec.time_linked:
            points = _window(points, query["start"], query["end"], state)
        if spec.mode == "grouped_points":
            grouped = defaultdict(list)
            group_index = int(spec.options.get("group_index", 2))
            for point in points:
                grouped["1" if bool(point[group_index]) else "0"].append(point)
            names = spec.options.get("groups", {})
            for key, values in grouped.items():
                input_points += len(values)
                series.append({"id": key, "name": names.get(key, key),
                               "points": _sample_points(values, query["max_points"])})
        else:
            input_points += len(points)
            points = _sample_points(points, query["max_points"], state)
            if points:
                series.append({"id": spec.id, "name": spec.title, "points": points})
    metadata["input_points"] = input_points
    if lookup:
        metadata["lookup"] = lookup
    return series, metadata


def _legacy_cache_series(csv_dir: Path, query: dict):
    frame = _read(csv_dir / "worker_storage_consumption.csv")
    columns = [name for name in frame.columns if name not in {"time", "workflow_completion_percentage"}]
    selected = set(query["series"])
    if selected:
        unknown = selected - set(columns)
        if unknown:
            raise ChartQueryError(f"unknown series: {', '.join(sorted(unknown))}")
        columns = [name for name in columns if name in selected]
    frame = frame.select(["time", *columns])
    if query["view"] == "total":
        totals = frame.select(columns).fill_null(strategy="forward").fill_null(0).select(
            pl.sum_horizontal(pl.all()).alias("total")
        )["total"].to_list()
        points = [[float(time), float(total) * 2**20] for time, total in zip(frame["time"], totals)]
        points = _window(points, query["start"], query["end"], True)
        count = len(points)
        return [{"id": "total", "name": "Total", "points": _sample_points(points, query["max_points"], True)}], {
            "legacy": True, "input_points": count,
        }
    series, count = [], 0
    for column in columns:
        points = [[float(x), float(y) * 2**20] for x, y in frame.select(["time", column]).drop_nulls().iter_rows()]
        points = _window(points, query["start"], query["end"], True)
        count += len(points)
        points = _sample_points(points, query["max_points"], True)
        if points:
            series.append({"id": column, "name": column, "points": points})
    return series, {"legacy": True, "input_points": count}


def _build_cache_occupancy(csv_dir: Path, query: dict):
    path = csv_dir / "metric_worker_cache_occupancy.csv"
    if not path.is_file():
        return _legacy_cache_series(csv_dir, query)
    frame = _read(path)
    if frame.height == 0:
        return [], {"input_points": 0}
    if query["evidence"] == "observed":
        frame = frame.filter(pl.col("evidence") == "observed")
    workers = sorted(str(value) for value in frame["worker_id"].unique().drop_nulls())
    selected = set(query["series"])
    if selected:
        unknown = selected - set(workers)
        if unknown:
            raise ChartQueryError(f"unknown series: {', '.join(sorted(unknown))}")
        workers = [worker for worker in workers if worker in selected]
        frame = frame.filter(pl.col("worker_id").is_in(workers))
    cache_spec = metric_spec("worker-storage-consumption")
    view = query["view"] or cache_spec.options.get("default_view", "total")
    if view not in {"by-worker", "total"}:
        raise ChartQueryError("view must be 'by-worker' or 'total'")
    metadata = {}
    if view == "by-worker" and not selected:
        limit = int(cache_spec.options.get("default_series_limit", 0))
        if limit and len(workers) > limit:
            ranking = (
                frame.filter(pl.col("delta_bytes") > 0)
                .group_by("worker_id").agg(pl.col("delta_bytes").sum().alias("cached_bytes"))
                .sort(["cached_bytes", "worker_id"], descending=[True, False])
            )
            available = len(workers)
            workers = [str(value) for value in ranking["worker_id"].head(limit)]
            frame = frame.filter(pl.col("worker_id").is_in(workers))
            metadata = {
                "available_series": available, "selected_series": len(workers),
                "omitted_series": available - len(workers),
                "series_selection": "highest cumulative cached bytes",
            }
    groups = [("total", "Total", frame)] if view == "total" else [
        (worker, worker, frame.filter(pl.col("worker_id") == worker)) for worker in workers
    ]
    series, input_points = [], 0
    for series_id, name, group in groups:
        states = (
            group.group_by("time").agg(pl.col("delta_bytes").sum()).sort("time")
            .with_columns(pl.col("delta_bytes").cum_sum().clip(0).alias("value"))
        )
        points = [[float(x), float(y)] for x, y in states.select(["time", "value"]).iter_rows()]
        points = _window(points, query["start"], query["end"], True)
        input_points += len(points)
        points = _sample_points(points, query["max_points"], True)
        if points:
            series.append({"id": series_id, "name": name, "points": points})
    return series, {
        "input_event_rows": frame.height, "input_state_points": input_points,
        "input_points": input_points, **metadata,
    }


@lru_cache(maxsize=256)
def _cached_payload(csv_dir_string: str, metric_id: str, query_items: tuple) -> dict:
    csv_dir = Path(csv_dir_string)
    spec = metric_spec(metric_id)
    if spec is None or spec.mode == "source":
        raise ChartQueryError(f"unknown standard metric: {metric_id}")
    query = dict(query_items)
    query["series"] = tuple(query.get("series", ()))
    if spec.mode == "cache_occupancy":
        series, metadata = _build_cache_occupancy(csv_dir, query)
    elif spec.mode == "custom":
        raise ChartQueryError(f"metric requires a custom endpoint: {metric_id}")
    else:
        series, metadata = _build_standard(csv_dir, spec, query)
    output_points = sum(len(item["points"]) for item in series)
    input_points = int(metadata.get("input_points", output_points))
    contract = _contract(csv_dir, spec)
    if metadata.get("legacy"):
        contract = {
            **contract, "status": "PARTIAL",
            "caveat": "Legacy cache projection; re-run vine_parse for residency evidence and reconciliation.",
        }
    return {
        "metric_id": metric_id, "available": bool(series), "contract": contract,
        "renderer": spec.renderer, "curve": spec.curve,
        "axes": {
            "x": {"unit": spec.x_unit, "domain": _extent(series, 0)},
            "y": {"unit": spec.y_unit, "domain": _extent(series, 1)},
        },
        "series": series, "query": query,
        "sampling": {
            "method": "state-first-min-max-last" if spec.curve == "step" else "endpoint-extrema-stride",
            "max_points_per_series": query["max_points"], "input_points": input_points,
            "output_points": output_points, "lossy": output_points < input_points,
        },
        "metadata": metadata,
        "point_fields": [spec.x_field, spec.y_field or "value", *spec.extra_fields],
    }


_FLIGHT_LOCKS = tuple(threading.Lock() for _ in range(32))


def chart_payload(snapshot, metric_id: str, query: dict) -> dict:
    immutable_query = tuple(sorted(query.items()))
    key = (str(snapshot.csv_files_dir), metric_id, immutable_query)
    with _FLIGHT_LOCKS[hash(key) % len(_FLIGHT_LOCKS)]:
        payload = _cached_payload(*key)
    return copy.deepcopy(payload)


def cache_occupancy_peak_bytes(csv_dir: Path | str) -> float | None:
    """Return the exact aggregate peak shared by graph and summary consumers."""
    query = {
        "start": None, "end": None, "series": (), "max_points": 5000,
        "view": "total", "evidence": "all",
    }
    series, _ = _build_cache_occupancy(Path(csv_dir), query)
    values = [point[1] for item in series for point in item.get("points", [])]
    return max(values) if values else None
