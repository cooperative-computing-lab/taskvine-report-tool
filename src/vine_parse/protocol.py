"""Stateless protocol classification shared by native and Python paths."""

import os
import re
from itertools import islice
from typing import Sequence


HANDLER_RULES = (
    ("busy_on", lambda line, parts: "busy on" in line),
    ("puturl", lambda line, parts: "puturl" in parts or "puturl_now" in parts),
    ("cache_update", lambda line, parts: "cache-update" in parts),
    ("task_state_change", lambda line, parts: "state change:" in line),
    ("unlink", lambda line, parts: "unlink" in parts),
    ("worker_removed", lambda line, parts: "removed" in parts and "worker" in parts),
    ("complete", lambda line, parts: "complete" in parts),
    ("worker_received", lambda line, parts: "received" in parts),
    ("receive_worker_info", lambda line, parts: " info " in line),
    ("cache_invalid", lambda line, parts: "cache-invalid" in parts),
    ("worker_resources", lambda line, parts: "resources" in parts),
    ("stdout", lambda line, parts: "stdout" in parts),
    ("recovery_task", lambda line, parts: "Submitted recovery task" in line),
    ("put_file", lambda line, parts: "put" in parts),
    ("failed_to_send_task", lambda line, parts: "Failed to send task" in line),
    ("transfer_port", lambda line, parts: "transfer-port" in parts),
    ("exhausted_resources", lambda line, parts: "exhausted" in parts and "resources" in parts),
    ("listening", lambda line, parts: "listening on port" in line),
    ("worker_connected", lambda line, parts: "worker" in parts and "connected" in parts),
    ("manager_end", lambda line, parts: "manager end" in line),
    ("remove_instances", lambda line, parts: "Removing instances of worker" in line),
    ("kill_task", lambda line, parts: " kill " in line),
    ("added_dependency", lambda line, parts: "added dependency" in line),
    ("send_task_to_worker", lambda line, parts: "tx to" in line),
)
HANDLER_NAMES = tuple(name for name, _ in HANDLER_RULES)
HANDLER_MASKS = {name: 1 << index for index, name in enumerate(HANDLER_NAMES)}
ALL_HANDLER_MASK = (1 << len(HANDLER_NAMES)) - 1
PROTOCOL_DIAGNOSTIC_RULES = (
    (("task_state_change",), r"state change:"),
    (("complete",), r"\bcomplete\b"),
    (("cache_update", "cache_invalid"), r"cache-(?:update|invalid)"),
    (("puturl",), r"\bputurl(?:_now)?\b"),
    (("worker_connected", "worker_removed"), r"\bworker\b.*\b(?:connected|removed)\b"),
    (("recovery_task",), r"Submitted recovery task"),
    (("transfer_port",), r"transfer-port"),
    (("busy_on",), r"busy on"),
    (("send_task_to_worker",), r"tx to.*\btask\b"),
    (("manager_end",), r"manager end"),
)
PROTOCOL_CANDIDATE_NAMES = frozenset(
    name for names, _ in PROTOCOL_DIAGNOSTIC_RULES for name in names
)
PROTOCOL_CANDIDATE_MASK = sum(HANDLER_MASKS[name] for name in PROTOCOL_CANDIDATE_NAMES)
PROTOCOL_CANDIDATE_RE = re.compile(
    "|".join(pattern for _, pattern in PROTOCOL_DIAGNOSTIC_RULES),
    re.IGNORECASE,
)
RULESET_VERSION = 1


def reference_candidate_mask(line: str, parts: list[str]) -> int:
    """Return a stateless superset of handlers that may accept a line."""
    tokens = set(parts)
    return sum(
        HANDLER_MASKS[name]
        for name, matches in HANDLER_RULES
        if matches(line, tokens)
    )


class CandidateClassifier:
    """Optional native prefilter with a semantics-preserving Python fallback."""

    def __init__(self):
        disabled = os.environ.get("TASKVINE_DISABLE_NATIVE_PARSER", "").lower()
        if disabled in {"1", "true", "yes"}:
            _debug_native = None
        else:
            try:
                from . import _debug_native
            except ImportError:
                _debug_native = None
        if _debug_native is not None:
            if tuple(_debug_native.HANDLER_NAMES) != HANDLER_NAMES:
                raise RuntimeError("native debug classifier handler table is incompatible")
            if _debug_native.RULESET_VERSION != RULESET_VERSION:
                raise RuntimeError("native debug classifier ruleset is incompatible")
        self._native = _debug_native

    @property
    def available(self) -> bool:
        return self._native is not None

    def classify_batch(self, lines: list[str]) -> Sequence[int]:
        if self._native is None:
            return [ALL_HANDLER_MASK] * len(lines)
        packed = self._native.classify_batch_packed(lines)
        return memoryview(packed).cast("I")

    def classified_records(self, records, batch_size: int = 4096):
        """Yield ``(record, mask)`` while bounding native-call overhead."""
        if self._native is None:
            for record in records:
                yield record, None
            return

        iterator = iter(records)
        while True:
            batch = list(islice(iterator, batch_size))
            if not batch:
                return
            masks = self.classify_batch([record.text or "" for record in batch])
            if len(masks) != len(batch):
                raise RuntimeError("native debug classifier returned an invalid batch")
            yield from zip(batch, masks)
