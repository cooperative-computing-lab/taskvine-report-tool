"""Adapter for the unversioned, human-readable TaskVine debug log."""

import re
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional


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


_TIMESTAMP_BYTES_RE = re.compile(rb"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+ ")
# The first line of a task description sent to a worker.  Upstream logs every
# later field as its own "tx to" record; some producers (the vine-graph fork)
# log the whole description as one record whose later fields follow on
# untimestamped lines.
_DESCRIPTION_HEAD_RE = re.compile(
    r"^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+ \S*vine: tx to \S+ \([^()\s]*\): )(?:task \d+|mini_task \S.*)$"
)
# Fields followed by a body of exactly n bytes: "cmd <n>" then the command
# with no newline of its own, and "env <n>" then the variable and a newline.
_LENGTH_FIELD_RE = re.compile(rb"^(?:cmd|env) (\d+)$")
# Upstream logs "cmd <n>" as its own record and then the command itself as the
# next record, "<timestamp> <process>vine: <command>\n", which may span lines.
_UPSTREAM_CMD_RE = re.compile(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+ \S*vine: tx to \S+ \([^()\s]*\): cmd (\d+)$")
_RECORD_HEADER_RE = re.compile(rb"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+ \S*vine: ")


def expand_task_descriptions(records: Iterable[SourceRecord]) -> Iterator[SourceRecord]:
    """Give every task-description field its own record and hide command bodies.

    Upstream TaskVine logs each field of a task description as its own
    "<timestamp> <process>vine: tx to <host> (<addr>): <field>" record, and the
    command after "cmd <n>" as a separate record.  The vine-graph fork logs the
    whole description as one record whose later fields follow on untimestamped
    lines.  Both become the upstream shape here, so one reducer handles both.

    Bodies announced by ``cmd <n>`` and ``env <n>`` are skipped by byte length
    and yielded with empty text, so a command without a trailing newline, one
    that spans lines, or one whose text looks like another message (a
    timestamp, "complete", "state change:") is never read as a record.  A body
    ends inside a physical line at most once, so each physical line yields one
    record and line accounting is unchanged.  A joined description ends at its
    ``end`` field; a timestamped line also ends it unless it lies inside an
    announced body.  Other records pass through unchanged.
    """
    prefix = None          # prefix of an open joined description, or None
    need = 0               # body bytes still to skip
    command_bytes = None   # length of an upstream command body expected next
    for record in records:
        raw = record.raw
        if command_bytes is not None:
            header = _RECORD_HEADER_RE.match(raw)
            if header:
                # The command and the newline the debug format adds.
                need = command_bytes + 1
                command_bytes = None
                position = header.end() + min(need, len(raw) - header.end())
                need -= position - header.end()
                yield SourceRecord(record.line_number, raw, "")
                continue
            command_bytes = None
        if need and prefix is None:
            # The rest of a multi-line upstream command body.
            position = min(need, len(raw))
            need -= position
            yield SourceRecord(record.line_number, raw, "")
            continue
        if prefix is not None and not (need == 0 and _TIMESTAMP_BYTES_RE.match(raw)):
            position = min(need, len(raw))
            need -= position
            rest = raw[position:].rstrip(b"\r\n")
            if need or not rest:
                # Body only, or the newline after an env body: no field here.
                yield SourceRecord(record.line_number, raw, "")
                continue
            try:
                field = rest.decode("utf-8")
            except UnicodeDecodeError as exc:
                yield SourceRecord(record.line_number, raw, None, str(exc))
                continue
            yield SourceRecord(record.line_number, raw, prefix + field)
            length = _LENGTH_FIELD_RE.match(rest)
            if length:
                need = int(length.group(1))
            elif field == "end":
                prefix = None
            continue
        prefix = None
        yield record
        if record.text is not None:
            head = _DESCRIPTION_HEAD_RE.match(record.text)
            if head:
                prefix = head.group(1)
                need = 0
                continue
            upstream_cmd = _UPSTREAM_CMD_RE.match(record.text)
            if upstream_cmd:
                command_bytes = int(upstream_cmd.group(1))
