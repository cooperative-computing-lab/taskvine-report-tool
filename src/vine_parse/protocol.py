"""Stateless protocol classification shared by native and Python paths."""

import os
from itertools import islice
from typing import Sequence


HANDLER_NAMES = (
    "busy_on",
    "puturl",
    "cache_update",
    "task_state_change",
    "unlink",
    "worker_removed",
    "complete",
    "worker_received",
    "receive_worker_info",
    "cache_invalid",
    "worker_resources",
    "stdout",
    "recovery_task",
    "put_file",
    "failed_to_send_task",
    "transfer_port",
    "exhausted_resources",
    "listening",
    "worker_connected",
    "manager_end",
    "remove_instances",
    "kill_task",
    "added_dependency",
    "send_task_to_worker",
)
HANDLER_MASKS = {name: 1 << index for index, name in enumerate(HANDLER_NAMES)}
ALL_HANDLER_MASK = (1 << len(HANDLER_NAMES)) - 1
RULESET_VERSION = 1


def reference_candidate_mask(line: str, parts: list[str]) -> int:
    """Return a stateless superset of handlers that may accept a line."""
    tokens = set(parts)
    mask = 0

    def mark(name, matches):
        nonlocal mask
        if matches:
            mask |= HANDLER_MASKS[name]

    mark("busy_on", "busy on" in line)
    mark("puturl", "puturl" in tokens or "puturl_now" in tokens)
    mark("cache_update", "cache-update" in tokens)
    mark("task_state_change", "state change:" in line)
    mark("unlink", "unlink" in tokens)
    mark("worker_removed", "removed" in tokens and "worker" in tokens)
    mark("complete", "complete" in tokens)
    mark("worker_received", "received" in tokens)
    mark("receive_worker_info", " info " in line)
    mark("cache_invalid", "cache-invalid" in tokens)
    mark("worker_resources", "resources" in tokens)
    mark("stdout", "stdout" in tokens)
    mark("recovery_task", "Submitted recovery task" in line)
    mark("put_file", "put" in tokens)
    mark("failed_to_send_task", "Failed to send task" in line)
    mark("transfer_port", "transfer-port" in tokens)
    mark("exhausted_resources", "exhausted" in tokens and "resources" in tokens)
    mark("listening", "listening on port" in line)
    mark("worker_connected", "worker" in tokens and "connected" in tokens)
    mark("manager_end", "manager end" in line)
    mark("remove_instances", "Removing instances of worker" in line)
    mark("kill_task", " kill " in line)
    mark("added_dependency", "added dependency" in line)
    # The stateful half of this condition is evaluated by DataParser.
    mark("send_task_to_worker", "tx to" in line)
    return mask


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
