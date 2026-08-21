#!/usr/bin/env python3
"""
vine_report command - Start TaskVine Report Web Server

This command starts the web-based visualization interface for TaskVine logs.
"""

import argparse
import json
import os
# Polars otherwise sizes its global pool from the login node's full CPU count.
# Bound it before any route imports Polars so multiple report processes remain
# safe under the cluster's cgroup thread limit.
_compute_threads = os.environ.get("VINE_REPORT_COMPUTE_THREADS", "4")
for _thread_variable in (
    "POLARS_MAX_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS", "OMP_NUM_THREADS",
):
    os.environ.setdefault(_thread_variable, _compute_threads)
import sys
import time
import warnings
import logging
import socket
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from flask import Flask, g, jsonify, render_template, request
from werkzeug.serving import ThreadedWSGIServer

# Suppress Flask development server warnings
warnings.filterwarnings("ignore", message="This is a development server.*")
# Suppress Werkzeug warnings
logging.getLogger("werkzeug").setLevel(logging.ERROR)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from src.vine_report.routes.worker_storage_consumption import (
    worker_storage_consumption_bp,
)
from src.vine_report.routes.file_sizes import file_sizes_bp
from src.vine_report.routes.file_concurrent_replicas import file_concurrent_replicas_bp
from src.vine_report.routes.task_concurrency import task_concurrency_bp
from src.vine_report.routes.task_execution_time import task_execution_time_bp
from src.vine_report.routes.task_execution_details import task_execution_details_bp
from src.vine_report.routes.task_response_time import task_response_time_bp
from src.vine_report.routes.task_retrieval_time import task_retrieval_time_bp
from src.vine_report.routes.task_dependents import task_dependents_bp
from src.vine_report.routes.worker_concurrency import worker_concurrency_bp
from src.vine_report.routes.worker_executing_tasks import worker_executing_tasks_bp
from src.vine_report.routes.worker_waiting_retrieval_tasks import (
    worker_waiting_retrieval_tasks_bp,
)
from src.vine_report.routes.worker_lifetime import worker_lifetime_bp
from src.vine_report.routes.file_transferred_size import file_transferred_size_bp
from src.vine_report.routes.file_created_size import file_created_size_bp
from src.vine_report.routes.file_retention_time import file_retention_time_bp
from src.vine_report.routes.worker_transfers import worker_transfers_bp
from src.vine_report.routes.task_completion_percentiles import (
    task_completion_percentiles_bp,
)
from src.vine_report.routes.task_dependencies import task_dependencies_bp
from src.vine_report.routes.task_subgraphs import task_subgraphs_bp
from src.vine_report.routes.export_csv_files import register_csv_export_routes
from src.vine_report.routes.dashboard import dashboard_bp
from src.vine_report.routes.api_v1 import api_v1_bp
from src.vine_report.routes.logger import Logger
from src.vine_report.run_store import CatalogLease, RunCatalog, RunStoreError, SnapshotStore
from src.vine_report.runtime_metrics import RuntimeMetrics
from src.vine_report.metric_registry import standard_metric_ids
from src.utils import check_pip_updates
from src import __version__


class BoundedThreadPoolWSGIServer(ThreadedWSGIServer):
    """Werkzeug WSGI server with a fixed request-thread budget.

    Werkzeug's development server creates one new thread per connection.  On
    shared login nodes that can exhaust the process/thread cgroup under bursty
    multi-window traffic.  A bounded executor applies backpressure instead.
    """

    def __init__(self, *args, max_workers=8, max_pending=None, **kwargs):
        max_pending = max_workers * 4 if max_pending is None else max_pending
        # Include active workers in the slot count. Once all slots are used,
        # the accept loop waits instead of growing ThreadPoolExecutor's
        # otherwise-unbounded work queue.
        self._request_slots = threading.BoundedSemaphore(max_workers + max_pending)
        self._request_executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="vine-report-request",
        )
        super().__init__(*args, **kwargs)

    def process_request(self, request_socket, client_address):
        self._request_slots.acquire()
        try:
            self._request_executor.submit(
                self._process_request_with_slot, request_socket, client_address
            )
        except Exception:
            self._request_slots.release()
            self.shutdown_request(request_socket)
            raise

    def _process_request_with_slot(self, request_socket, client_address):
        try:
            self.process_request_thread(request_socket, client_address)
        finally:
            self._request_slots.release()

    def server_close(self):
        super().server_close()
        self._request_executor.shutdown(wait=True, cancel_futures=True)


def create_app(
    logs_dir,
    downsample_task_bars=100000,
    downsample_points=10000,
    request_threads=8,
    pending_requests=None,
):
    package_dir = os.path.dirname(__file__)  # vine_report/
    template_dir = os.path.join(package_dir, "templates")
    static_dir = os.path.join(package_dir, "static")

    app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
    logger = Logger()
    app.config["APP_LOGGER"] = logger
    app.config["RUN_CATALOG"] = RunCatalog(logs_dir)
    app.config["SNAPSHOT_STORE"] = SnapshotStore(app.config["RUN_CATALOG"], logger)
    pending_requests = request_threads * 4 if pending_requests is None else pending_requests
    app.config["RUNTIME_METRICS"] = RuntimeMetrics(request_threads, pending_requests)
    logger.info(f"APP Using logs directory: {app.config['RUN_CATALOG'].root}")

    def setup_request_logging(app):
        @app.before_request
        def log_request_info():
            g.request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
            request._start_time = time.time()
            app.config["RUNTIME_METRICS"].request_started()
            app.config["APP_LOGGER"].info(
                f"request_id={g.request_id} method={request.method} path={request.path}"
            )

        @app.after_request
        def log_response_info(response):
            duration = time.time() - getattr(request, "_start_time", time.time())
            snapshot = getattr(g, "run_snapshot", None)
            identity = (
                f" run_id={snapshot.run_id} revision={snapshot.revision}"
                if snapshot is not None else ""
            )
            app.config["APP_LOGGER"].info(
                f"request_id={getattr(g, 'request_id', '-')} status={response.status_code} "
                f"duration_ms={duration * 1000:.2f}{identity}"
            )
            response.headers.setdefault("X-Request-ID", getattr(g, "request_id", ""))
            app.config["RUNTIME_METRICS"].request_finished(response.status_code)
            return response

        return app

    setup_request_logging(app)

    # Register blueprints
    # Tasks
    app.register_blueprint(task_execution_details_bp)
    app.register_blueprint(task_execution_time_bp)
    app.register_blueprint(task_concurrency_bp)
    app.register_blueprint(task_response_time_bp)
    app.register_blueprint(task_retrieval_time_bp)
    app.register_blueprint(task_completion_percentiles_bp)
    app.register_blueprint(task_dependencies_bp)
    app.register_blueprint(task_dependents_bp)

    # Workers
    app.register_blueprint(worker_storage_consumption_bp)
    app.register_blueprint(worker_concurrency_bp)
    app.register_blueprint(worker_transfers_bp)
    app.register_blueprint(worker_executing_tasks_bp)
    app.register_blueprint(worker_waiting_retrieval_tasks_bp)
    app.register_blueprint(worker_lifetime_bp)

    # Files
    app.register_blueprint(file_concurrent_replicas_bp)
    app.register_blueprint(file_sizes_bp)
    app.register_blueprint(file_transferred_size_bp)
    app.register_blueprint(file_created_size_bp)
    app.register_blueprint(file_retention_time_bp)

    # Subgraphs
    app.register_blueprint(task_subgraphs_bp)

    app.register_blueprint(dashboard_bp)

    # Set sampling parameters
    app.config["DOWNSAMPLE_TASK_BARS"] = downsample_task_bars
    app.config["DOWNSAMPLE_POINTS"] = downsample_points

    @app.route("/")
    def index():
        return render_template(
            "index.html",
            log_folders=app.config["RUN_CATALOG"].list_runs(),
        )

    register_csv_export_routes(app)

    app.config["CHART_ENDPOINTS"] = {
        rule.rule.removeprefix("/api/"): rule.endpoint
        for rule in app.url_map.iter_rules()
        if rule.rule.startswith("/api/")
        and "<" not in rule.rule
        and "/" not in rule.rule.removeprefix("/api/")
    }
    for metric_id in standard_metric_ids():
        app.config["CHART_ENDPOINTS"].setdefault(metric_id, None)
    app.register_blueprint(api_v1_bp)

    @app.errorhandler(RunStoreError)
    def handle_run_store_error(error):
        return jsonify({"error": {"code": error.code, "message": error.message}}), error.status

    return app


def get_local_ip_addresses():
    ip_addresses = ["127.0.0.1"]

    try:
        hostname = socket.gethostname()

        for addr_info in socket.getaddrinfo(hostname, None):
            ip = addr_info[4][0]
            if ":" not in ip and ip != "127.0.0.1" and ip not in ip_addresses:
                ip_addresses.append(ip)
    except Exception:
        pass

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            if local_ip not in ip_addresses:
                ip_addresses.append(local_ip)
    except Exception:
        pass

    return ip_addresses


def _gc_main(argv):
    parser = argparse.ArgumentParser(
        prog="vine_report gc",
        description="Safely inspect or collect old immutable report snapshots",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--logs-dir", default=os.getcwd())
    parser.add_argument("--keep", type=int, default=3, help="Most recently used revisions to retain per run")
    parser.add_argument("--min-age-hours", type=float, default=24.0, help="Minimum revision age before collection")
    parser.add_argument("--run", action="append", dest="runs", help="Limit collection to a run; repeatable")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Only print the collection plan (default)")
    mode.add_argument("--apply", action="store_true", help="Apply the collection plan")
    args = parser.parse_args(argv)
    catalog = RunCatalog(os.path.abspath(args.logs_dir))
    # Maintenance should not initialize or truncate the interactive server log.
    store = SnapshotStore(catalog, logging.getLogger("taskvine-report-gc"))
    try:
        result = store.collect(
            keep=args.keep,
            min_age_seconds=args.min_age_hours * 3600,
            dry_run=not args.apply,
            run_ids=args.runs,
        )
    except RunStoreError as error:
        parser.error(f"{error.code}: {error.message}")
    print(json.dumps(result, indent=2, sort_keys=True))


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "gc":
        _gc_main(sys.argv[2:])
        return
    parser = argparse.ArgumentParser(
        prog="vine_report",
        description="Start TaskVine Report web server for log visualization",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--logs-dir",
        default=os.getcwd(),
        help="Directory containing log folders (default: current directory)",
    )

    parser.add_argument(
        "--downsample-task-bars",
        default=50000,
        type=int,
        help="",
    )

    parser.add_argument(
        "--downsample-points",
        default=1000,
        type=int,
        help="",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=9122,
        help="Port number for the web server (default: 9122)",
    )

    parser.add_argument(
        "--threads",
        type=int,
        default=8,
        help="Maximum concurrent HTTP request threads",
    )

    parser.add_argument(
        "--host", default="0.0.0.0", help="Host address to bind to (default: 0.0.0.0)"
    )

    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s {__version__}"
    )

    args = parser.parse_args()

    if args.threads < 1 or args.threads > 64:
        parser.error("--threads must be between 1 and 64")

    logs_dir = os.path.abspath(args.logs_dir)
    try:
        server_lease = CatalogLease(Path(logs_dir), exclusive=False)
    except RunStoreError as error:
        parser.error(f"{error.code}: {error.message}")
    check_pip_updates()
    print(f"🚀 Starting TaskVine Report server...")
    print(f"   📁 Logs directory: {logs_dir}")
    print(f"   🌐 Server accessible at:")

    app = create_app(
        logs_dir,
        downsample_task_bars=args.downsample_task_bars,
        downsample_points=args.downsample_points,
        request_threads=args.threads,
    )

    if args.host == "0.0.0.0":
        ip_addresses = get_local_ip_addresses()
        for ip in ip_addresses:
            if ip == "127.0.0.1":
                print(f"      🔗 http://localhost:{args.port}")
            else:
                print(f"      🔗 http://{ip}:{args.port}")
    else:
        print(f"      🔗 http://{args.host}:{args.port}")

    print(f"\nPress Ctrl+C to stop the server")

    server = BoundedThreadPoolWSGIServer(
        args.host, args.port, app, max_workers=args.threads
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)
    finally:
        server.server_close()
        server_lease.close()


if __name__ == "__main__":
    main()
