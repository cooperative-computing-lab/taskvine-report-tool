"""Input adapters for TaskVine log formats.

Adapters isolate unstable producer text from the report's domain parser.  A new
TaskVine log format should result in a new adapter, not conditionals spread
through the report and UI layers.
"""

from .debug_legacy import LegacyDebugSource, SourceRecord
from .transactions_legacy import LegacyTransactionSource, TransactionRecord

__all__ = [
    "LegacyDebugSource",
    "LegacyTransactionSource",
    "SourceRecord",
    "TransactionRecord",
]
