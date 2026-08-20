"""Reader for TaskVine's current unversioned transaction log.

This deliberately exposes records as evidence only.  The format has no schema
version and is not treated as a safe replacement for the debug log.
"""

from dataclasses import dataclass
from typing import Iterator, Optional


KNOWN_ENTITIES = {"MANAGER", "WORKER", "CATEGORY", "TASK", "LIBRARY", "APPLICATION"}


@dataclass(frozen=True)
class TransactionRecord:
    line_number: int
    raw: str
    timestamp_us: Optional[int]
    manager_pid: Optional[int]
    entity: Optional[str]
    payload: str
    error: Optional[str] = None


class LegacyTransactionSource:
    adapter_id = "taskvine-transactions-legacy-v1"
    format_name = "taskvine.transactions.unversioned"

    def __init__(self, path: str):
        self.path = path

    def records(self) -> Iterator[TransactionRecord]:
        with open(self.path, "r", encoding="utf-8", errors="replace") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                line = raw_line.rstrip("\n")
                if not line or line.startswith("#"):
                    continue
                parts = line.split(maxsplit=3)
                if len(parts) < 3:
                    yield TransactionRecord(line_number, line, None, None, None, "", "too_few_fields")
                    continue
                try:
                    timestamp_us = int(parts[0])
                    manager_pid = int(parts[1])
                except ValueError:
                    yield TransactionRecord(line_number, line, None, None, None, "", "invalid_prefix")
                    continue
                entity = parts[2]
                payload = parts[3] if len(parts) == 4 else ""
                error = None if entity in KNOWN_ENTITIES else "unknown_entity"
                yield TransactionRecord(
                    line_number,
                    line,
                    timestamp_us,
                    manager_pid,
                    entity,
                    payload,
                    error,
                )
