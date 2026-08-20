"""Request-local access to immutable report snapshots."""

from flask import current_app, g, request

from .run_store import RunStoreError


def bind_snapshot(run_id: str, revision: str | None = None):
    snapshot = current_app.config["SNAPSHOT_STORE"].open(run_id, revision)
    g.run_snapshot = snapshot
    return snapshot


def get_run_snapshot():
    snapshot = getattr(g, "run_snapshot", None)
    if snapshot is not None:
        return snapshot
    run_id = request.args.get("folder") or request.args.get("run_id")
    revision = request.args.get("revision")
    if not run_id:
        raise RunStoreError("missing_run_id", "folder or run_id is required")
    return bind_snapshot(run_id, revision)
