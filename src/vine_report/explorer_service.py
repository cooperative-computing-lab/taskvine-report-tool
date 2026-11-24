"""Revision-local entity summaries for the dashboard explorers."""

from __future__ import annotations

import csv
import math
from collections import defaultdict
from functools import lru_cache
from pathlib import Path


TASK_RECORDS = {"successful_tasks", "unsuccessful_tasks"}


def _rows(csv_dir: Path, name: str):
    path = csv_dir / name
    if not path.is_file():
        return
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        yield from csv.DictReader(handle)


def _number(value, cast=float):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return int(number) if cast is int else number


def _file_names(value):
    return tuple(name.strip() for name in str(value or "").split(",") if name.strip())


def _worker_endpoint(value):
    value = str(value or "").strip()
    prefix, separator, suffix = value.rpartition(":")
    return prefix if separator and suffix.isdigit() and ":" in prefix else value


def _limited(values, limit=20):
    return sorted(values, key=str)[:limit]


@lru_cache(maxsize=8)
def file_records(csv_dir_value: str) -> tuple[dict, ...]:
    csv_dir = Path(csv_dir_value)
    files = {}

    def file(name):
        return files.setdefault(name, {
            "file_name": name,
            "file_id": None,
            "size_bytes": None,
            "retention_seconds": None,
            "max_replicas": None,
            "activation_count": 0,
            "producer_tasks": set(),
            "consumer_tasks": set(),
            "source_workers": set(),
        })

    for row in _rows(csv_dir, "file_sizes.csv"):
        name = row.get("file_name", "").strip()
        if not name:
            continue
        item = file(name)
        item["file_id"] = _number(row.get("file_idx"), int)
        size_kb = _number(row.get("file_size_kb"))
        item["size_bytes"] = size_kb * 1024 if size_kb is not None else None

    for filename, field, target in (
        ("file_retention_time.csv", "retention_time", "retention_seconds"),
        ("file_concurrent_replicas.csv", "max_simul_replicas", "max_replicas"),
    ):
        for row in _rows(csv_dir, filename):
            name = row.get("file_name", "").strip()
            if name:
                file(name)[target] = _number(row.get(field), int if target == "max_replicas" else float)

    for row in _rows(csv_dir, "file_replica_activation_intervals.csv"):
        name = row.get("filename", "").strip()
        if not name:
            continue
        item = file(name)
        item["activation_count"] += 1
        worker = row.get("source_worker", "").strip()
        if worker:
            item["source_workers"].add(worker)

    for row in _rows(csv_dir, "task_execution_details.csv"):
        if row.get("record_type") not in TASK_RECORDS:
            continue
        task_id = _number(row.get("task_id"), int)
        for name in _file_names(row.get("input_files")):
            if task_id is not None:
                file(name)["consumer_tasks"].add(task_id)
        for name in _file_names(row.get("output_files")):
            if task_id is not None:
                file(name)["producer_tasks"].add(task_id)

    result = []
    for item in files.values():
        produced = bool(item["producer_tasks"])
        consumed = bool(item["consumer_tasks"])
        item["kind"] = "intermediate" if produced and consumed else "output" if produced else "input" if consumed else "unreferenced"
        for field in ("producer_tasks", "consumer_tasks", "source_workers"):
            values = item[field]
            item[f"{field}_count"] = len(values)
            item[field] = _limited(values)
        result.append(item)
    result.sort(key=lambda item: (-(item["size_bytes"] or -1), item["file_name"]))
    return tuple(result)


@lru_cache(maxsize=8)
def worker_records(csv_dir_value: str) -> tuple[dict, ...]:
    csv_dir = Path(csv_dir_value)
    workers = {}
    endpoint_by_id = {}

    def worker(endpoint):
        endpoint = _worker_endpoint(endpoint)
        return workers.setdefault(endpoint, {
            "worker": endpoint,
            "worker_id": None,
            "lifetime_seconds": None,
            "tasks": 0,
            "successful_tasks": 0,
            "unsuccessful_tasks": 0,
            "execution_seconds": 0.0,
            "execution_samples": 0,
            "max_cores": None,
            "categories": set(),
            "files_cached": set(),
            "cache_bytes": 0.0,
            "peak_cache_bytes": 0.0,
        })

    for row in _rows(csv_dir, "worker_lifetime.csv"):
        endpoint = _worker_endpoint(row.get("Worker IP Port"))
        if not endpoint:
            continue
        item = worker(endpoint)
        worker_id = _number(row.get("ID"), int)
        item["worker_id"] = worker_id
        item["lifetime_seconds"] = _number(row.get("LifeTime (s)"))
        if worker_id is not None:
            endpoint_by_id[worker_id] = endpoint

    for row in _rows(csv_dir, "task_execution_details.csv"):
        if row.get("record_type") not in TASK_RECORDS:
            continue
        worker_id = _number(row.get("worker_id"), int)
        endpoint = endpoint_by_id.get(worker_id) or _worker_endpoint(row.get("worker_entry"))
        if not endpoint:
            continue
        item = worker(endpoint)
        if item["worker_id"] is None:
            item["worker_id"] = worker_id
        item["tasks"] += 1
        success = row.get("record_type") == "successful_tasks"
        item["successful_tasks" if success else "unsuccessful_tasks"] += 1
        execution = _number(row.get("execution_time"))
        if execution is not None:
            item["execution_seconds"] += execution
            item["execution_samples"] += 1
        cores = _number(row.get("cores_requested"), int)
        if cores is not None:
            item["max_cores"] = max(item["max_cores"] or 0, cores)
        category = row.get("category", "").strip()
        if category:
            item["categories"].add(category)

    # Parser output is chronological, so occupancy can be accumulated in one pass.
    for row in _rows(csv_dir, "metric_worker_cache_occupancy.csv"):
        endpoint = _worker_endpoint(row.get("worker_id"))
        if not endpoint:
            continue
        item = worker(endpoint)
        item["cache_bytes"] = max(0.0, item["cache_bytes"] + (_number(row.get("delta_bytes")) or 0))
        item["peak_cache_bytes"] = max(item["peak_cache_bytes"], item["cache_bytes"])
        name = row.get("file_name", "").strip()
        if name:
            item["files_cached"].add(name)

    result = []
    for item in workers.values():
        item["success_rate"] = item["successful_tasks"] / item["tasks"] if item["tasks"] else None
        samples = item.pop("execution_samples")
        item["average_execution_seconds"] = item["execution_seconds"] / samples if samples else None
        if not samples:
            item["execution_seconds"] = None
        item["status"] = "issues" if item["unsuccessful_tasks"] else "productive" if item["tasks"] else "idle"
        for field in ("categories", "files_cached"):
            values = item[field]
            item[f"{field}_count"] = len(values)
            item[field] = _limited(values)
        result.append(item)
    result.sort(key=lambda item: (-item["tasks"], -(item["lifetime_seconds"] or -1), item["worker"]))
    return tuple(result)
