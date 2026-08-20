"""Adapter for the unversioned, human-readable TaskVine debug log."""

from dataclasses import dataclass
from typing import Iterator, Optional


@dataclass(frozen=True)
class SourceRecord:
    line_number: int
    raw: bytes
    text: Optional[str]
    decode_error: Optional[str] = None


class LegacyDebugSource:
    """Stream legacy debug records without imposing domain interpretation."""

    adapter_id = "taskvine-debug-legacy-v1"
    format_name = "taskvine.debug.unversioned"

    def __init__(self, path: str):
        self.path = path

    def records(self) -> Iterator[SourceRecord]:
        with open(self.path, "rb") as handle:
            for line_number, raw in enumerate(handle, start=1):
                try:
                    text = raw.decode("utf-8").strip()
                    yield SourceRecord(line_number, raw, text)
                except UnicodeDecodeError as exc:
                    yield SourceRecord(line_number, raw, None, str(exc))
