"""Shared time-domain and boundary semantics for chart series."""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True, slots=True)
class TimeContext:
    domain: tuple[float, float] | None
    debug_offset: float = 0.0

    def offset_for(self, origin: str) -> float:
        return self.debug_offset if origin == "debug" else 0.0


def _finite(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _metadata(csv_dir: Path) -> dict:
    path = csv_dir / "metadata.csv"
    if not path.is_file():
        return {}
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        result = {}
        for row in csv.DictReader(handle):
            value = row.get("value")
            try:
                result[row.get("key")] = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                result[row.get("key")] = value
        return result


def _debug_domain(csv_dir: Path) -> tuple[float, float] | None:
    path = csv_dir / "time_domain.csv"
    if not path.is_file():
        return None
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        row = next(csv.DictReader(handle), None)
    if not row:
        return None
    start = _finite(row.get("MIN_TIME"))
    end = _finite(row.get("MAX_TIME"))
    return (start, end) if start is not None and end is not None and end >= start else None


@lru_cache(maxsize=256)
def time_context(csv_dir_string: str) -> TimeContext:
    csv_dir = Path(csv_dir_string)
    metadata = _metadata(csv_dir)
    debug = _debug_domain(csv_dir)
    manager_start = _finite(metadata.get("manager_start_time"))
    manager_end = _finite(metadata.get("manager_end_time"))
    duration = _finite(metadata.get("manager_duration"))
    if duration is None and manager_start is not None and manager_end is not None:
        duration = manager_end - manager_start
    if duration is None and debug is not None:
        duration = debug[1] - debug[0]
    if duration is None or duration <= 0:
        return TimeContext(None)

    # Production debug artifacts use epoch timestamps in time_domain.csv and
    # are relative to its MIN_TIME. Small values are legacy relative fixtures.
    debug_offset = 0.0
    if debug is not None and manager_start is not None and debug[0] > 100_000_000:
        debug_offset = max(0.0, debug[0] - manager_start)
    return TimeContext((0.0, float(duration)), debug_offset)


def shift_points(points: list[list], offset: float) -> list[list]:
    shifted = []
    for point in points:
        if len(point) < 2:
            continue
        x, y = _finite(point[0]), _finite(point[1])
        if x is not None and y is not None:
            shifted.append([x + offset, y, *point[2:]])
    return sorted(shifted, key=lambda point: point[0])


def _at(points: list[list], x: float, step: bool = True) -> float | None:
    previous = None
    for point in points:
        if point[0] > x:
            if previous is None:
                return None
            if step:
                return previous[1]
            span = point[0] - previous[0]
            ratio = 0.0 if span == 0 else (x - previous[0]) / span
            return previous[1] + ratio * (point[1] - previous[1])
        previous = point
    return previous[1] if previous is not None else None


def normalize_state_points(points: list[list], domain: tuple[float, float]) -> list[list]:
    """Cover a complete state domain with an explicit zero baseline."""
    start, end = domain
    ordered = [point for point in points if start <= point[0] <= end]
    start_value = _at(points, start)
    if start_value is None:
        start_value = 0.0
    if not ordered or ordered[0][0] > start:
        ordered.insert(0, [start, start_value])
    elif ordered[0][0] == start:
        ordered[0] = [start, ordered[0][1], *ordered[0][2:]]

    end_value = _at(points, end)
    if end_value is None:
        end_value = ordered[-1][1] if ordered else 0.0
    if not ordered or ordered[-1][0] < end:
        ordered.append([end, end_value])
    return ordered


def normalize_bucket_points(
    points: list[list], domain: tuple[float, float], width: float,
) -> list[list]:
    """Materialize every rate/count bucket, including explicit empty buckets."""
    start, end = domain
    width = max(float(width), 1e-9)
    bucket_count = max(1, int(math.ceil((end - start) / width)))
    values = {}
    for point in points:
        index = int(round((point[0] - start) / width))
        if 0 <= index < bucket_count:
            values[index] = point[1]
    result = [[start + index * width, values.get(index, 0.0)] for index in range(bucket_count)]
    result.append([end, result[-1][1]])
    return result


def normalize_temporal_points(
    points: list[list], context: TimeContext, *, origin: str, policy: str | None,
    bucket_width: float | None = None,
) -> list[list]:
    shifted = shift_points(points, context.offset_for(origin))
    if context.domain is None or not policy:
        return shifted
    if policy == "state":
        return normalize_state_points(shifted, context.domain)
    if policy == "bucket_zero":
        width = bucket_width or max(1.0, math.ceil(context.domain[1] / 400))
        return normalize_bucket_points(shifted, context.domain, width)
    return shifted
