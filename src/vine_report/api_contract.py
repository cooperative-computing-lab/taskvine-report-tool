"""Stable public identifiers for the versioned report API."""

API_SCHEMA = "report-api/v1"
API_VERSION = "1"
API_CAPABILITIES = ("source-backed-metrics-v1", "metric-registry-v1", "entity-explorers-v1", "revision-watch-v1")

# Additive response fields are allowed within v1. Removing or renaming these
# envelope fields or changing endpoint identity semantics requires API v2.
SUCCESS_ENVELOPE_FIELDS = ("schema_version", "request_id", "data")
PINNED_ENVELOPE_FIELDS = SUCCESS_ENVELOPE_FIELDS + ("run_id", "revision")
ERROR_ENVELOPE_FIELDS = ("schema_version", "request_id", "error")
