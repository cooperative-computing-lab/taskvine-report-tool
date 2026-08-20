"""Durable compatibility diagnostics for unversioned TaskVine logs."""

import hashlib
import json
import os
import re
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from .sources import LegacyTransactionSource


DIAGNOSTICS_SCHEMA_VERSION = 1
PARSER_CONTRACT_VERSION = 1
DIAGNOSTICS_FILENAME = "parse-diagnostics.json"
MANIFEST_FILENAME = "parse-manifest.json"

_TIMESTAMP_RE = re.compile(r"^\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?")
_TIMEZONE_RE = re.compile(r"manager timezone at startup:.*\btz_offset=([+-]\d{4})\b")
_CCTOOLS_VERSION_RE = re.compile(r"\brunning CCTools version\s+([^\s]+)")
_PROTOCOL_CANDIDATE_RE = re.compile(
    r"state change:|\bcomplete\b|cache-(?:update|invalid)|\bputurl(?:_now)?\b|"
    r"\bworker\b.*\b(?:connected|removed)\b|Submitted recovery task|"
    r"transfer-port|busy on|tx to.*\btask\b|manager end",
    re.IGNORECASE,
)
_NUMBER_RE = re.compile(r"(?<![A-Za-z])[-+]?\d+(?:\.\d+)?")
_ADDRESS_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}:\d+\b")
_PID_RE = re.compile(r"\[[0-9]+\]")
_LONG_ID_RE = re.compile(r"\b[A-Fa-f0-9]{12,}\b")


def installed_tool_version() -> str:
    try:
        from importlib.metadata import version
    except ImportError:  # Python 3.7
        try:
            from importlib_metadata import version
        except ImportError:
            return "unknown"
    try:
        return version("taskvine-report-tool")
    except Exception:
        return "unknown"


class LogCompatibilityError(ValueError):
    """A log record matched a known message family but violated its contract."""

    def __init__(self, line_number: int, handler: str, message: str):
        super().__init__(
            "legacy debug contract violation at line {} in handler '{}': {}".format(
                line_number, handler, message
            )
        )
        self.line_number = line_number
        self.handler = handler


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json_atomic(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".{}-".format(path.name), dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, str(path))
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def normalized_shape(line: str) -> str:
    """Return a bounded, non-payload structural signature for a debug line."""
    value = _TIMESTAMP_RE.sub("<timestamp>", line)
    value = _PID_RE.sub("[<pid>]", value)
    value = _ADDRESS_RE.sub("<address>", value)
    value = re.sub(r"\b(?:https?|file|workerip)://\S+", "<url>", value)
    value = re.sub(r"(?<!\w)/(?:[^\s,;]+)", "<path>", value)
    value = re.sub(r"(['\"]).*?\1", "<quoted>", value)
    value = _LONG_ID_RE.sub("<id>", value)
    value = _NUMBER_RE.sub("<n>", value)
    value = re.sub(r"\{.*\}", "<object>", value)
    return value[:320]


def infer_timezone_offset(debug_path: Path, transactions_path: Path) -> Optional[int]:
    """Infer the debug wall-clock offset from a transaction-log epoch.

    Both logs record manager startup.  The inferred value is accepted only when
    it is a valid 15-minute timezone offset and the residual is at most two
    minutes, so unrelated or concatenated runs fail closed.
    """
    if not debug_path.is_file() or not transactions_path.is_file():
        return None

    listening_times = []
    with debug_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if "listening on port" not in line:
                continue
            match = _TIMESTAMP_RE.match(line)
            if match:
                try:
                    listening_times.append(datetime.strptime(match.group(0), "%Y/%m/%d %H:%M:%S.%f"))
                except ValueError:
                    pass

    start_epochs = []
    for record in LegacyTransactionSource(str(transactions_path)).records():
        if record.entity != "MANAGER" or record.timestamp_us is None:
            continue
        fields = record.payload.split()
        if len(fields) >= 2 and fields[1] == "START":
            start_epochs.append(record.timestamp_us / 1e6)

    if not listening_times or not start_epochs:
        return None

    # Prefer the latest start from each log.  This also matches the legacy
    # cleaner, which keeps the final manager run from a concatenated debug log.
    naive_epoch = listening_times[-1].replace(tzinfo=timezone.utc).timestamp()
    raw_offset = naive_epoch - start_epochs[-1]
    rounded = int(round(raw_offset / 900.0) * 900)
    if abs(raw_offset - rounded) > 120 or not (-23 * 3600 - 59 * 60 <= rounded <= 23 * 3600 + 59 * 60):
        return None
    return rounded


def inspect_transactions(path: Path) -> Dict:
    result = {
        "present": path.is_file(),
        "path": str(path),
        "format": LegacyTransactionSource.format_name,
        "adapter": LegacyTransactionSource.adapter_id,
        "records": 0,
        "malformed_records": 0,
        "entities": {},
    }
    if not path.is_file():
        return result

    entities = Counter()
    malformed = 0
    for record in LegacyTransactionSource(str(path)).records():
        result["records"] += 1
        if record.entity:
            entities[record.entity] += 1
        if record.error:
            malformed += 1
    result["malformed_records"] = malformed
    result["entities"] = dict(sorted(entities.items()))
    result["sha256"] = sha256_file(path)
    result["size_bytes"] = path.stat().st_size
    result["contract_status"] = "unversioned_evidence_only"
    return result


class ParseDiagnostics:
    def __init__(self, runtime_template: str, source_path: str, tool_version: str):
        self.runtime_template = Path(runtime_template)
        self.source_path = Path(source_path)
        self._source_digest = hashlib.sha256()
        self._handler_counts = Counter()
        self._shapes = set()
        self._partial_reasons = []
        self._error_samples = []
        self._cctools_versions = set()
        self._timezone_offsets = set()
        self.data = {
            "schema_version": DIAGNOSTICS_SCHEMA_VERSION,
            "parser_contract_version": PARSER_CONTRACT_VERSION,
            "tool_version": tool_version,
            "phase": "parse",
            "status": "RUNNING",
            "started_at": utc_now(),
            "runtime_template": str(self.runtime_template.resolve()),
            "source": {
                "path": str(self.source_path.resolve()),
                "format": "taskvine.debug.unversioned",
                "adapter": "taskvine-debug-legacy-v1",
                "total_lines": 0,
                "timestamped_lines": 0,
                "handled_lines": 0,
                "unhandled_lines": 0,
                "protocol_candidate_unhandled_lines": 0,
                "decode_errors": 0,
            },
            "handler_counts": {},
            "compatibility": {
                "upstream_schema_version": None,
                "mode": "legacy_unversioned",
                "limitations": [
                    "TaskVine debug text has no machine-readable schema version",
                    "compatibility is inferred and validated by this adapter",
                ],
            },
            "partial_reasons": [],
            "errors": [],
        }

    @property
    def path(self) -> Path:
        return self.runtime_template / "vine-logs" / DIAGNOSTICS_FILENAME

    def select_source(self, path: str) -> None:
        self.source_path = Path(path)
        self.data["source"]["path"] = str(self.source_path.resolve())

    def record_raw(self, raw: bytes) -> None:
        self._source_digest.update(raw)
        self.data["source"]["total_lines"] += 1

    def record_decode_error(self, line_number: int, message: str) -> None:
        self.data["source"]["decode_errors"] += 1
        self._add_error("decode_error", line_number, None, message)

    def record_timestamped(self) -> None:
        self.data["source"]["timestamped_lines"] += 1

    def record_metadata(self, line: str) -> None:
        self._cctools_versions.update(_CCTOOLS_VERSION_RE.findall(line))
        match = _TIMEZONE_RE.search(line)
        if match:
            self._timezone_offsets.add(match.group(1))

    def set_timezone_evidence(self, method: str, offset_seconds: int) -> None:
        self.data["compatibility"]["timezone"] = {
            "method": method,
            "offset_seconds": offset_seconds,
        }

    def record_handled(self, handler: str, line: str) -> None:
        self.data["source"]["handled_lines"] += 1
        self._handler_counts[handler] += 1
        self._shapes.add("{}:{}".format(handler, normalized_shape(line)))

    def record_unhandled(self, line_number: int, line: str) -> None:
        self.data["source"]["unhandled_lines"] += 1
        if _PROTOCOL_CANDIDATE_RE.search(line):
            self.data["source"]["protocol_candidate_unhandled_lines"] += 1
            if len(self._error_samples) < 20:
                self._error_samples.append(
                    {
                        "kind": "unhandled_protocol_candidate",
                        "line": line_number,
                        "shape": normalized_shape(line),
                    }
                )

    def record_contract_error(self, line_number: int, handler: str, message: str) -> None:
        self._add_error("contract_violation", line_number, handler, message)

    def note_partial(self, reason: str) -> None:
        if reason not in self._partial_reasons:
            self._partial_reasons.append(reason)

    def _add_error(self, kind: str, line_number: Optional[int], handler: Optional[str], message: str) -> None:
        if len(self._error_samples) >= 20:
            return
        self._error_samples.append(
            {
                "kind": kind,
                "line": line_number,
                "handler": handler,
                "message": normalized_shape(str(message))[:500],
            }
        )

    def finalize(self, error: Optional[BaseException] = None, stage: str = "parse") -> Dict:
        source = self.data["source"]
        source["sha256"] = self._source_digest.hexdigest()
        if self.source_path.exists():
            source["size_bytes"] = self.source_path.stat().st_size
            if source["total_lines"] == 0:
                source["sha256"] = sha256_file(self.source_path)
        source["format_fingerprint_sha256"] = hashlib.sha256(
            "\n".join(sorted(self._shapes)).encode("utf-8")
        ).hexdigest()
        self.data["handler_counts"] = dict(sorted(self._handler_counts.items()))
        source["cctools_versions_observed"] = sorted(self._cctools_versions)
        source["timezone_offsets_observed"] = sorted(self._timezone_offsets)
        self.data["partial_reasons"] = list(self._partial_reasons)
        self.data["errors"] = list(self._error_samples)
        self.data["companion_transactions"] = inspect_transactions(
            self.runtime_template / "vine-logs" / "transactions"
        )
        if error is not None:
            self._add_error("fatal", getattr(error, "line_number", None), getattr(error, "handler", None), str(error))
            self.data["errors"] = list(self._error_samples)
            self.data["status"] = "FAIL"
            self.data["failed_stage"] = stage
        elif self._partial_reasons or source["protocol_candidate_unhandled_lines"] or source["decode_errors"]:
            self.data["status"] = "PARTIAL"
        else:
            self.data["status"] = "PASS"
        self.data["finished_at"] = utc_now()
        return self.data

    def write(self) -> Path:
        write_json_atomic(self.path, self.data)
        return self.path


def inspect_run(runtime_template: str, tool_version: str, persist: bool = True) -> Dict:
    run = Path(runtime_template).expanduser().resolve()
    debug_path = run / "vine-logs" / "debug"
    result = {
        "schema_version": DIAGNOSTICS_SCHEMA_VERSION,
        "parser_contract_version": PARSER_CONTRACT_VERSION,
        "tool_version": tool_version,
        "phase": "preflight",
        "status": "PASS",
        "started_at": utc_now(),
        "runtime_template": str(run),
        "source": {
            "path": str(debug_path),
            "format": "taskvine.debug.unversioned",
            "adapter": "taskvine-debug-legacy-v1",
        },
        "compatibility": {
            "upstream_schema_version": None,
            "mode": "legacy_unversioned",
        },
        "partial_reasons": [],
        "errors": [],
    }
    if not debug_path.is_file():
        result["status"] = "FAIL"
        result["errors"].append({"kind": "missing_source", "message": "debug log not found"})
    else:
        digest = hashlib.sha256()
        lines = timestamped = decode_errors = 0
        timezone_offsets = set()
        versions = set()
        with debug_path.open("rb") as handle:
            for raw in handle:
                lines += 1
                digest.update(raw)
                try:
                    line = raw.decode("utf-8")
                except UnicodeDecodeError:
                    decode_errors += 1
                    continue
                if _TIMESTAMP_RE.match(line):
                    timestamped += 1
                timezone_match = _TIMEZONE_RE.search(line)
                if timezone_match:
                    timezone_offsets.add(timezone_match.group(1))
                versions.update(_CCTOOLS_VERSION_RE.findall(line))
        result["source"].update(
            {
                "sha256": digest.hexdigest(),
                "size_bytes": debug_path.stat().st_size,
                "total_lines": lines,
                "timestamped_lines": timestamped,
                "decode_errors": decode_errors,
                "timezone_offsets": sorted(timezone_offsets),
                "cctools_versions_observed": sorted(versions),
            }
        )
        if lines == 0 or timestamped == 0:
            result["status"] = "FAIL"
            result["errors"].append({"kind": "empty_or_unrecognized_source", "message": "no timestamped debug records found"})
        elif not timezone_offsets:
            inferred = infer_timezone_offset(debug_path, run / "vine-logs" / "transactions")
            if inferred is None:
                result["status"] = "PARTIAL"
                result["partial_reasons"].append("missing_timezone_contract")
            else:
                result["compatibility"]["timezone"] = {
                    "method": "inferred_from_transactions",
                    "offset_seconds": inferred,
                }
        if len(timezone_offsets) > 1:
            result["status"] = "PARTIAL"
            result["partial_reasons"].append("multiple_timezone_offsets")
        if decode_errors:
            result["status"] = "PARTIAL" if result["status"] != "FAIL" else "FAIL"
            result["partial_reasons"].append("undecodable_records")
    result["companion_transactions"] = inspect_transactions(run / "vine-logs" / "transactions")
    result["finished_at"] = utc_now()
    if persist:
        write_json_atomic(run / "vine-logs" / DIAGNOSTICS_FILENAME, result)
    return result


def write_parse_manifest(runtime_template: str, diagnostics: Dict) -> Path:
    run = Path(runtime_template).resolve()
    csv_dir = run / "csv-files"
    outputs = []
    if csv_dir.is_dir():
        for path in sorted(csv_dir.glob("*.csv")):
            outputs.append(
                {
                    "path": path.name,
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
            )
    payload = {
        "schema_version": 1,
        "created_at": utc_now(),
        "parser_contract_version": PARSER_CONTRACT_VERSION,
        "tool_version": diagnostics.get("tool_version"),
        "status": diagnostics.get("status"),
        "source": {
            key: diagnostics.get("source", {}).get(key)
            for key in ("path", "format", "adapter", "sha256", "format_fingerprint_sha256")
        },
        "diagnostics": str((run / "vine-logs" / DIAGNOSTICS_FILENAME).resolve()),
        "outputs": outputs,
    }
    path = csv_dir / MANIFEST_FILENAME
    write_json_atomic(path, payload)
    return path
