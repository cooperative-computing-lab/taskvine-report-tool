"""Stateless, revision-pinned TaskVine Report API."""

from __future__ import annotations

from pathlib import Path
import os
import platform

from flask import Blueprint, current_app, g, jsonify, make_response, request, send_file

from src.vine_report.request_context import bind_snapshot
from src.vine_report.run_store import RunStoreError
from src.vine_report.api_contract import API_CAPABILITIES, API_SCHEMA, API_VERSION
from src.vine_report.metric_service import metric_payload
from src.vine_report.chart_service import ChartQueryError, chart_payload, normalize_query
from src.vine_report.metric_registry import metric_catalog, metric_spec, standard_metric_ids
from src.vine_parse.diagnostics import PARSER_CONTRACT_VERSION
from src import __version__
from .dashboard import (
    DashboardAPIError,
    _comparison_from_summaries,
    _summary,
    files as dashboard_files,
    tasks as dashboard_tasks,
    workers as dashboard_workers,
)


api_v1_bp = Blueprint("api_v1", __name__, url_prefix="/api/v1")


def _envelope(data, snapshot=None):
    payload = {
        "schema_version": API_SCHEMA,
        "request_id": getattr(g, "request_id", None),
        "data": data,
    }
    if snapshot is not None:
        payload.update({"run_id": snapshot.run_id, "revision": snapshot.revision})
    return payload


def _error(error: RunStoreError | DashboardAPIError):
    return jsonify({
        "schema_version": API_SCHEMA,
        "request_id": getattr(g, "request_id", None),
        "error": {"code": error.code, "message": error.message},
    }), error.status


@api_v1_bp.errorhandler(RunStoreError)
def handle_run_store_error(error):
    return _error(error)


@api_v1_bp.errorhandler(DashboardAPIError)
def handle_dashboard_error(error):
    return _error(error)


@api_v1_bp.after_request
def identity_headers(response):
    response.headers["X-TaskVine-API-Version"] = API_VERSION
    response.headers["X-Request-ID"] = getattr(g, "request_id", "")
    snapshot = getattr(g, "run_snapshot", None)
    if snapshot is not None:
        response.headers["X-TaskVine-Run"] = snapshot.run_id
        response.headers["X-TaskVine-Revision"] = snapshot.revision
        response.set_etag(snapshot.revision)
    return response


@api_v1_bp.get("/runs")
def runs():
    return jsonify(_envelope({"runs": current_app.config["RUN_CATALOG"].list_runs()}))


@api_v1_bp.get("/catalog")
def catalog():
    return jsonify(_envelope({"metrics": metric_catalog()}))


@api_v1_bp.get("/health")
def health():
    catalog = current_app.config["RUN_CATALOG"]
    data = {
        "status": "ok" if catalog.root.is_dir() else "degraded",
        "service": "taskvine-report-tool",
        "version": __version__,
        "api_schema": API_SCHEMA,
        "capabilities": list(API_CAPABILITIES),
        "parser_contract_version": PARSER_CONTRACT_VERSION,
        "run_count": len(catalog.list_runs()),
        "runtime": current_app.config["RUNTIME_METRICS"].snapshot(),
    }
    return jsonify(_envelope(data)), 200 if data["status"] == "ok" else 503


@api_v1_bp.get("/diagnostics")
def diagnostics():
    return jsonify(_envelope({
        "service": {
            "name": "taskvine-report-tool",
            "version": __version__,
            "api_schema": API_SCHEMA,
            "capabilities": list(API_CAPABILITIES),
            "parser_contract_version": PARSER_CONTRACT_VERSION,
        },
        "runtime": {
            **current_app.config["RUNTIME_METRICS"].snapshot(),
            "compute_threads": int(os.environ.get("VINE_REPORT_COMPUTE_THREADS", "4")),
            "python": platform.python_version(),
        },
        "runs": current_app.config["SNAPSHOT_STORE"].describe_runs(),
    }))


@api_v1_bp.get("/compare")
def compare():
    run_ids = list(dict.fromkeys(
        item.strip() for item in request.args.get("runs", "").split(",") if item.strip()
    ))
    if len(run_ids) < 2:
        raise DashboardAPIError("invalid_parameter", "at least two runs are required")
    if len(run_ids) > 4:
        raise DashboardAPIError("invalid_parameter", "at most four runs can be compared")
    summaries = []
    for run_id in run_ids:
        snapshot = current_app.config["SNAPSHOT_STORE"].refresh(run_id)
        g.run_snapshot = snapshot
        summaries.append((_summary(run_id), snapshot.revision))
    data = _comparison_from_summaries([summary for summary, _ in summaries])
    for item, (_, revision) in zip(data["runs"], summaries):
        item["revision"] = revision
    g.pop("run_snapshot", None)
    return jsonify(_envelope(data))


@api_v1_bp.get("/runs/<run_id>")
def run_descriptor(run_id):
    store = current_app.config["SNAPSHOT_STORE"]
    snapshot = store.refresh(run_id)
    g.run_snapshot = snapshot
    return jsonify(_envelope({
        "run_id": run_id,
        "current_revision": snapshot.revision,
        "source_token": store.source_token(run_id),
    }, snapshot))


@api_v1_bp.get("/runs/<run_id>/watch")
def watch_run(run_id):
    store = current_app.config["SNAPSHOT_STORE"]
    return jsonify(_envelope({
        "run_id": run_id,
        "current_revision": store.current_revision(run_id, refresh=False),
        "source_token": store.source_token(run_id),
    }))


@api_v1_bp.post("/runs/<run_id>/refresh")
def refresh(run_id):
    store = current_app.config["SNAPSHOT_STORE"]
    snapshot = store.refresh(run_id)
    g.run_snapshot = snapshot
    return jsonify(_envelope({
        "run_id": run_id,
        "current_revision": snapshot.revision,
        "source_token": store.source_token(run_id),
    }, snapshot))


def _pinned(run_id, revision):
    return bind_snapshot(run_id, revision)


@api_v1_bp.get("/runs/<run_id>/revisions/<revision>/summary")
def summary(run_id, revision):
    snapshot = _pinned(run_id, revision)
    return jsonify(_envelope(_summary(run_id), snapshot))


def _dashboard_entity(run_id, revision, view):
    snapshot = _pinned(run_id, revision)
    raw = make_response(view())
    data = raw.get_json(silent=True)
    if raw.status_code >= 400:
        return raw
    return jsonify(_envelope(data, snapshot))


@api_v1_bp.get("/runs/<run_id>/revisions/<revision>/tasks")
def tasks(run_id, revision):
    return _dashboard_entity(run_id, revision, dashboard_tasks)


@api_v1_bp.get("/runs/<run_id>/revisions/<revision>/files")
def files(run_id, revision):
    return _dashboard_entity(run_id, revision, dashboard_files)


@api_v1_bp.get("/runs/<run_id>/revisions/<revision>/workers")
def workers(run_id, revision):
    return _dashboard_entity(run_id, revision, dashboard_workers)


@api_v1_bp.get("/runs/<run_id>/revisions/<revision>/metrics/<metric_id>")
def metric(run_id, revision, metric_id):
    snapshot = _pinned(run_id, revision)
    spec = metric_spec(metric_id)
    if spec is not None and spec.mode != "source":
        try:
            data = chart_payload(
                snapshot, metric_id,
                normalize_query(request.args, current_app.config["DOWNSAMPLE_POINTS"]),
            )
        except ChartQueryError as error:
            raise RunStoreError("invalid_parameter", str(error), 400) from error
    else:
        data = metric_payload(snapshot, metric_id)
    if data is None:
        raise RunStoreError("metric_not_found", f"metric '{metric_id}' was not found", 404)
    return jsonify(_envelope(data, snapshot))


@api_v1_bp.get("/runs/<run_id>/revisions/<revision>/charts/<chart_id>")
def chart(run_id, revision, chart_id):
    snapshot = _pinned(run_id, revision)
    endpoint = current_app.config["CHART_ENDPOINTS"].get(chart_id)
    if endpoint is None:
        raise RunStoreError("chart_not_found", f"chart '{chart_id}' was not found", 404)
    raw = make_response(current_app.view_functions[endpoint]())
    data = raw.get_json(silent=True)
    if raw.status_code >= 400:
        message = data.get("error", "chart request failed") if isinstance(data, dict) else "chart request failed"
        return _error(RunStoreError("chart_request_failed", str(message), raw.status_code))
    return jsonify(_envelope(data, snapshot))


@api_v1_bp.get("/runs/<run_id>/revisions/<revision>/exports/<chart_id>.csv")
def export(run_id, revision, chart_id):
    snapshot = _pinned(run_id, revision)
    allowed = current_app.config["CHART_ENDPOINTS"]
    if chart_id not in allowed and chart_id not in standard_metric_ids():
        raise RunStoreError("chart_not_found", f"chart '{chart_id}' was not found", 404)
    filename = chart_id.replace("-", "_") + ".csv"
    path = Path(snapshot.csv_files_dir) / filename
    if not path.is_file():
        raise RunStoreError("export_not_found", "CSV export is unavailable for this chart", 404)
    return send_file(
        path,
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
        conditional=True,
        etag=f"{snapshot.revision}-{chart_id}",
    )
