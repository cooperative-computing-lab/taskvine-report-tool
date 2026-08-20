"""Immutable, request-addressable report snapshots.

The report server must never have a process-wide "current run".  This module
turns the mutable parser output into content-addressed snapshots and resolves
every request by ``run_id`` and ``revision``.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import tempfile
import threading
import time
import uuid
from pathlib import Path

from src.vine_parse.csv_manager import CSVManager

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback keeps serving available
    fcntl = None


class RunStoreError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


class CatalogLease:
    """Cross-process shared server / exclusive maintenance lease."""

    def __init__(self, root: Path, exclusive: bool):
        self.handle = None
        if not root.is_dir():
            raise RunStoreError(
                "logs_dir_not_found", f"logs directory '{root}' was not found", 404
            )
        if fcntl is None:
            if exclusive:
                raise RunStoreError(
                    "gc_lock_unsupported",
                    "safe snapshot collection requires POSIX file locking",
                    501,
                )
            return
        self.handle = (root / ".vine-report.lease").open("a+")
        operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        try:
            fcntl.flock(self.handle.fileno(), operation | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self.handle.close()
            self.handle = None
            raise RunStoreError(
                "gc_server_active",
                "snapshot collection is blocked while vine_report is serving this logs directory",
                409,
            ) from error

    def close(self) -> None:
        if self.handle is not None:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def _json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class RunCatalog:
    """Validate run identifiers and contain all access under one logs root."""

    def __init__(self, logs_dir: str):
        self.root = Path(logs_dir).expanduser().resolve()

    def resolve(self, run_id: str) -> Path:
        if not run_id or run_id != Path(run_id).name or run_id in {".", ".."}:
            raise RunStoreError("invalid_run_id", "run_id must be a run name")
        candidate = (self.root / run_id).resolve()
        if candidate.parent != self.root or not candidate.is_dir():
            raise RunStoreError("run_not_found", f"run '{run_id}' was not found", 404)
        if not (candidate / "csv-files" / "metadata.csv").is_file():
            raise RunStoreError("run_data_unavailable", "run has no report data", 422)
        return candidate

    def list_runs(self) -> list[str]:
        if not self.root.is_dir():
            return []
        return sorted(
            path.name
            for path in self.root.iterdir()
            if path.is_dir() and (path / "csv-files" / "metadata.csv").is_file()
        )


class RunSnapshot(CSVManager):
    """Read-only CSVManager-compatible view of one immutable revision."""

    def __init__(self, run_id: str, revision: str, path: Path, logger):
        self.run_id = run_id
        self.revision = revision
        self.source_path = path
        self.logger = logger
        super().__init__(str(path))
        # CSVManager also owns an SVG render cache.  Keep that mutable cache
        # outside the content-addressed snapshot so a read can never mutate
        # the revision it is meant to reproduce.
        self.svg_files_dir = str(
            path.parent.parent / "cache" / revision / "svg-files"
        )
        Path(self.svg_files_dir).mkdir(parents=True, exist_ok=True)
        self.metadata = self.load_metadata_to_dict()

    def reload_data_if_needed(self):
        return False

    def log_info(self, message):
        self.logger.info(f"[{self.run_id}@{self.revision[:12]}] {message}")

    def log_warning(self, message):
        self.logger.warning(f"[{self.run_id}@{self.revision[:12]}] {message}")

    def log_error(self, message, with_traceback=True):
        del with_traceback
        self.logger.error(f"[{self.run_id}@{self.revision[:12]}] {message}")


class SnapshotStore:
    """Materialize and open immutable report snapshots.

    Parser output remains backward compatible at ``run/csv-files``.  Refresh
    copies and verifies that output before atomically publishing a revision.
    Readers only use the immutable copy.
    """

    def __init__(self, catalog: RunCatalog, logger):
        self.catalog = catalog
        self.logger = logger
        self._locks_guard = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}
        self._cache: dict[tuple[str, str], RunSnapshot] = {}
        self._cache_guard = threading.Lock()
        self._last_access_touch: dict[tuple[str, str], float] = {}

    def _lock_for(self, run_id: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(run_id, threading.Lock())

    def _touch_access(self, run_path: Path, run_id: str, revision: str) -> None:
        key = (run_id, revision)
        now = time.monotonic()
        with self._cache_guard:
            if now - self._last_access_touch.get(key, 0) < 60:
                return
            self._last_access_touch[key] = now
        access = self._snapshot_root(run_path) / "access" / revision
        access.parent.mkdir(parents=True, exist_ok=True)
        access.touch(exist_ok=True)

    def describe_runs(self) -> list[dict]:
        """Return bounded, non-mutating operator metadata for every run."""
        descriptions = []
        for run_id in self.catalog.list_runs():
            run_path = self.catalog.resolve(run_id)
            report_root = self._snapshot_root(run_path)
            current = None
            pointer = report_root / "current.json"
            if pointer.is_file():
                try:
                    current = json.loads(pointer.read_text(encoding="utf-8")).get("revision")
                except (OSError, json.JSONDecodeError):
                    current = None
            snapshots = []
            for path in sorted((report_root / "snapshots").glob("*")):
                manifest_path = path / "manifest.json"
                if not path.is_dir() or len(path.name) != 64 or not manifest_path.is_file():
                    continue
                try:
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                    size_bytes = sum(
                        int(item.get("size_bytes", 0)) for item in manifest.get("outputs", [])
                    )
                except (OSError, TypeError, ValueError, json.JSONDecodeError):
                    size_bytes = None
                access = report_root / "access" / path.name
                stamp_path = access if access.exists() else manifest_path
                snapshots.append({
                    "revision": path.name,
                    "current": path.name == current,
                    "size_bytes": size_bytes,
                    "last_access_unix": stamp_path.stat().st_mtime,
                })
            diagnostics_path = run_path / "vine-logs" / "parse-diagnostics.json"
            quality = "UNKNOWN"
            parser_contract_version = None
            if diagnostics_path.is_file():
                try:
                    diagnostics = json.loads(diagnostics_path.read_text(encoding="utf-8"))
                    quality = diagnostics.get("status", "UNKNOWN")
                    parser_contract_version = diagnostics.get("parser_contract_version")
                except (OSError, json.JSONDecodeError):
                    quality = "UNKNOWN"
            descriptions.append({
                "run_id": run_id,
                "current_revision": current,
                "snapshot_count": len(snapshots),
                "snapshot_bytes": sum(item["size_bytes"] or 0 for item in snapshots),
                "data_quality": quality,
                "parser_contract_version": parser_contract_version,
                "snapshots": sorted(
                    snapshots, key=lambda item: item["last_access_unix"], reverse=True
                ),
            })
        return descriptions

    def collect(
        self,
        *,
        keep: int = 3,
        min_age_seconds: float = 24 * 3600,
        dry_run: bool = True,
        run_ids: list[str] | None = None,
    ) -> dict:
        """Plan or safely remove old immutable revisions and their caches.

        GC takes an exclusive catalog lease. Report servers hold shared leases,
        so collection fails closed instead of racing an active request.
        """
        if keep < 1:
            raise RunStoreError("invalid_gc_policy", "keep must be at least 1")
        if min_age_seconds < 0:
            raise RunStoreError("invalid_gc_policy", "minimum age cannot be negative")
        selected = run_ids or self.catalog.list_runs()
        unknown = sorted(set(selected) - set(self.catalog.list_runs()))
        if unknown:
            raise RunStoreError("run_not_found", f"run '{unknown[0]}' was not found", 404)
        result = {
            "dry_run": dry_run,
            "keep": keep,
            "min_age_seconds": min_age_seconds,
            "runs": [],
            "candidate_revisions": 0,
            "reclaimable_bytes": 0,
            "removed_revisions": 0,
            "reclaimed_bytes": 0,
        }
        with CatalogLease(self.catalog.root, exclusive=True):
            descriptions = {
                item["run_id"]: item for item in self.describe_runs()
                if item["run_id"] in selected
            }
            now = time.time()
            for run_id in selected:
                description = descriptions[run_id]
                ordered = description["snapshots"]
                protected = {item["revision"] for item in ordered[:keep]}
                if description["current_revision"]:
                    protected.add(description["current_revision"])
                candidates = [
                    item for item in ordered
                    if item["revision"] not in protected
                    and now - item["last_access_unix"] >= min_age_seconds
                ]
                run_result = {
                    "run_id": run_id,
                    "current_revision": description["current_revision"],
                    "protected_revisions": sorted(protected),
                    "candidates": [item["revision"] for item in candidates],
                    "reclaimable_bytes": sum(item["size_bytes"] or 0 for item in candidates),
                }
                if not dry_run:
                    run_path = self.catalog.resolve(run_id)
                    report_root = self._snapshot_root(run_path)
                    trash = report_root / "trash"
                    trash.mkdir(parents=True, exist_ok=True)
                    with self._lock_for(run_id):
                        for item in candidates:
                            revision = item["revision"]
                            source = report_root / "snapshots" / revision
                            if not source.is_dir():
                                continue
                            target = trash / f"{revision}-{uuid.uuid4().hex}"
                            source.rename(target)
                            with self._cache_guard:
                                self._cache.pop((run_id, revision), None)
                                self._last_access_touch.pop((run_id, revision), None)
                            shutil.rmtree(target)
                            shutil.rmtree(report_root / "cache" / revision, ignore_errors=True)
                            try:
                                (report_root / "access" / revision).unlink()
                            except FileNotFoundError:
                                pass
                result["runs"].append(run_result)
                result["candidate_revisions"] += len(candidates)
                result["reclaimable_bytes"] += run_result["reclaimable_bytes"]
                if not dry_run:
                    result["removed_revisions"] += len(candidates)
                    result["reclaimed_bytes"] += run_result["reclaimable_bytes"]
        return result

    @staticmethod
    def _source_files(run_path: Path) -> list[tuple[str, Path]]:
        files = []
        csv_dir = run_path / "csv-files"
        manifest_path = csv_dir / "parse-manifest.json"
        if manifest_path.is_file():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                outputs = manifest["outputs"]
            except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
                raise RunStoreError("parse_manifest_invalid", "parse manifest is invalid", 409) from error
            for entry in outputs:
                name = entry.get("path")
                if not name or name != Path(name).name:
                    raise RunStoreError("parse_manifest_invalid", "parse manifest contains an invalid path", 409)
                path = csv_dir / name
                if (
                    not path.is_file()
                    or path.stat().st_size != entry.get("size_bytes")
                    or _sha256(path) != entry.get("sha256")
                ):
                    raise RunStoreError(
                        "parse_in_progress",
                        "report outputs do not match the committed parse manifest; retry after parsing finishes",
                        409,
                    )
                files.append((f"csv-files/{name}", path))
            files.append(("csv-files/parse-manifest.json", manifest_path))
        else:
            for path in sorted(csv_dir.glob("*.csv") if csv_dir.is_dir() else []):
                if path.is_file():
                    files.append((f"csv-files/{path.name}", path))
        diagnostics = run_path / "vine-logs" / "parse-diagnostics.json"
        if diagnostics.is_file():
            if manifest_path.is_file():
                try:
                    diagnostics_data = json.loads(diagnostics.read_text(encoding="utf-8"))
                    manifest_source = manifest.get("source", {}).get("sha256")
                    diagnostic_source = diagnostics_data.get("source", {}).get("sha256")
                    if manifest_source and diagnostic_source and manifest_source != diagnostic_source:
                        raise RunStoreError(
                            "parse_in_progress",
                            "parse diagnostics and manifest describe different source logs",
                            409,
                        )
                except json.JSONDecodeError as error:
                    raise RunStoreError("parse_diagnostics_invalid", "parse diagnostics are invalid", 409) from error
            files.append(("vine-logs/parse-diagnostics.json", diagnostics))
        return files

    @staticmethod
    def _inventory(files: list[tuple[str, Path]]) -> list[dict]:
        return [
            {"path": relative, "size_bytes": path.stat().st_size, "sha256": _sha256(path)}
            for relative, path in files
        ]

    @staticmethod
    def _revision(inventory: list[dict]) -> str:
        canonical = json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(canonical).hexdigest()

    @staticmethod
    def _snapshot_root(run_path: Path) -> Path:
        return run_path / ".vine-report"

    def refresh(self, run_id: str) -> RunSnapshot:
        run_path = self.catalog.resolve(run_id)
        with self._lock_for(run_id):
            source_files = self._source_files(run_path)
            if not source_files:
                raise RunStoreError("run_data_unavailable", "run has no snapshot inputs", 422)
            inventory = self._inventory(source_files)
            revision = self._revision(inventory)
            report_root = self._snapshot_root(run_path)
            snapshots = report_root / "snapshots"
            destination = snapshots / revision
            if not destination.is_dir():
                staging_root = report_root / "staging"
                staging_root.mkdir(parents=True, exist_ok=True)
                temporary = Path(tempfile.mkdtemp(prefix="snapshot-", dir=str(staging_root)))
                try:
                    for entry, (_, source) in zip(inventory, source_files):
                        target = temporary / entry["path"]
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(source, target)
                        if target.stat().st_size != entry["size_bytes"] or _sha256(target) != entry["sha256"]:
                            raise RunStoreError(
                                "snapshot_source_changed",
                                "report data changed while a snapshot was being created; retry refresh",
                                409,
                            )
                    _json_atomic(temporary / "manifest.json", {
                        "schema_version": 1,
                        "run_id": run_id,
                        "revision": revision,
                        "outputs": inventory,
                    })
                    snapshots.mkdir(parents=True, exist_ok=True)
                    try:
                        temporary.rename(destination)
                    except OSError:
                        if destination.is_dir():
                            shutil.rmtree(temporary, ignore_errors=True)
                        else:
                            raise
                except Exception:
                    shutil.rmtree(temporary, ignore_errors=True)
                    raise
            _json_atomic(report_root / "current.json", {
                "schema_version": 1,
                "run_id": run_id,
                "revision": revision,
            })
            return self.open(run_id, revision)

    def current_revision(self, run_id: str, refresh: bool = True) -> str:
        if refresh:
            return self.refresh(run_id).revision
        run_path = self.catalog.resolve(run_id)
        pointer = self._snapshot_root(run_path) / "current.json"
        if not pointer.is_file():
            return self.refresh(run_id).revision
        try:
            payload = json.loads(pointer.read_text(encoding="utf-8"))
            return payload["revision"]
        except (OSError, KeyError, json.JSONDecodeError) as error:
            raise RunStoreError("snapshot_pointer_invalid", "run snapshot pointer is invalid", 500) from error

    def open(self, run_id: str, revision: str | None = None) -> RunSnapshot:
        run_path = self.catalog.resolve(run_id)
        if revision is None:
            return self.refresh(run_id)
        if len(revision) != 64 or any(ch not in "0123456789abcdef" for ch in revision):
            raise RunStoreError("invalid_revision", "revision must be a SHA-256 digest")
        path = self._snapshot_root(run_path) / "snapshots" / revision
        manifest_path = path / "manifest.json"
        if not path.is_dir() or not manifest_path.is_file():
            raise RunStoreError("revision_not_found", "requested report revision was not found", 404)
        key = (run_id, revision)
        with self._cache_guard:
            cached = self._cache.get(key)
        if cached is not None:
            self._touch_access(run_path, run_id, revision)
            return cached
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RunStoreError("snapshot_manifest_invalid", "snapshot manifest is invalid", 500) from error
        if manifest.get("run_id") != run_id or manifest.get("revision") != revision:
            raise RunStoreError("snapshot_identity_mismatch", "snapshot identity does not match its path", 500)
        outputs = manifest.get("outputs")
        if not isinstance(outputs, list) or self._revision(outputs) != revision:
            raise RunStoreError("snapshot_manifest_invalid", "snapshot manifest digest is invalid", 500)
        root = path.resolve()
        for entry in outputs:
            target = (path / entry.get("path", "")).resolve()
            if root not in target.parents or not target.is_file():
                raise RunStoreError("snapshot_manifest_invalid", "snapshot output path is invalid", 500)
            if target.stat().st_size != entry.get("size_bytes") or _sha256(target) != entry.get("sha256"):
                raise RunStoreError("snapshot_integrity_failed", "snapshot output failed integrity verification", 500)
        with self._cache_guard:
            snapshot = self._cache.get(key)
            if snapshot is None:
                snapshot = RunSnapshot(run_id, revision, path, self.logger)
                self._cache[key] = snapshot
        self._touch_access(run_path, run_id, revision)
        return snapshot
