"""Deterministic two-run report server used by browser acceptance tests."""

import argparse
import csv
import tempfile
from pathlib import Path

from src.vine_report.report import BoundedThreadPoolWSGIServer, create_app


def write_csv(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def make_run(root: Path, run_id: str, scale: int):
    csv_dir = root / run_id / "csv-files"
    metadata = [
        ("total_workers", 4 * scale), ("total_tasks", 3),
        ("total_all_tasks", 3), ("total_library_tasks", 0),
        ("successful_tasks", 2), ("unsuccessful_tasks", 1),
        ("recovery_tasks", 0), ("recovery_successful", 0),
        ("recovery_unsuccessful", 0), ("total_files", 3),
        ("manager_start_time", 1_700_000_000),
        ("manager_end_time", 1_700_000_030 * scale),
        ("manager_duration", 30 * scale),
    ]
    write_csv(csv_dir / "metadata.csv", ["key", "value"], metadata)
    write_csv(csv_dir / "time_domain.csv", ["MIN_TIME", "MAX_TIME"], [[0, 30 * scale]])
    write_csv(
        csv_dir / "task_execution_time.csv",
        ["Global Index", "Execution Time", "Task ID", "Task Try ID", "Ran to Completion"],
        [[1, 2 * scale, 1, 1, 1], [2, 5 * scale, 2, 1, 1], [3, 8 * scale, 3, 1, 0]],
    )
    write_csv(
        csv_dir / "task_response_time.csv",
        ["Global Index", "Response Time", "Task ID", "Task Try ID", "Was Dispatched"],
        [[1, 3 * scale, 1, 1, 1], [2, 6 * scale, 2, 1, 1], [3, 9 * scale, 3, 1, 1]],
    )
    write_csv(
        csv_dir / "task_retrieval_time.csv",
        ["Global Index", "Retrieval Time", "Task ID", "Task Try ID"],
        [[1, 0.2, 1, 1], [2, 0.4, 2, 1], [3, 0.6, 3, 1]],
    )
    write_csv(
        csv_dir / "task_concurrency.csv",
        ["time", "workflow_completion_percentage", "Waiting", "Committing", "Executing", "Retrieving", "Done"],
        [[0, 0, 3, 0, 0, 0, 0], [10, 33, 1, 0, 2, 0, 0], [30, 100, 0, 0, 0, 0, 3]],
    )
    task_header = [
        "record_type", "task_id", "task_try_id", "worker_entry", "worker_id", "core_id",
        "cores_requested", "is_recovery_task", "input_files", "output_files",
        "num_input_files", "num_output_files", "task_status", "category", "when_ready",
        "when_running", "time_worker_start", "time_worker_end", "execution_time",
        "when_waiting_retrieval", "when_retrieved", "when_failure_happens", "when_done",
        "unsuccessful_checkbox_name", "hash", "time_connected", "time_disconnected",
        "cores", "memory_mb", "disk_mb", "gpus",
    ]
    write_csv(csv_dir / "task_execution_details.csv", task_header, [
        ["successful_tasks", 1, 1, "worker-a", 1, 1, 1, False, "input-a", "output-a", 1, 1, 0, "alpha", 0, 1, 1, 3, 2, 3, 3.2, "", 3.3, "", "", "", "", "", "", "", ""],
        ["successful_tasks", 2, 1, "worker-b", 2, 1, 1, False, "input-b", "output-b", 1, 1, 0, "beta", 0, 2, 2, 7, 5, 7, 7.2, "", 7.3, "", "", "", "", "", "", "", ""],
        ["unsuccessful_tasks", 3, 1, "worker-b", 2, 1, 1, False, "input-c", "output-c", 1, 1, 40, "gamma", 0, 3, 3, 11, 8, 11, 11.2, 11.1, 11.3, "failure", "", "", "", "", "", "", ""],
    ])
    write_csv(csv_dir / "worker_concurrency.csv", ["time", "Active Workers (count)"], [[0, 1], [5, 4 * scale], [30, 0]])
    write_csv(csv_dir / "worker_lifetime.csv", ["ID", "Worker IP Port", "LifeTime (s)"], [[1, "worker-a", 25], [2, "worker-b", 30]])
    write_csv(csv_dir / "worker_storage_consumption.csv", ["time", "workflow_completion_percentage", "worker-a", "worker-b"], [[0, 0, 1, 2], [30, 100, 10 * scale, 20 * scale]])
    write_csv(csv_dir / "file_transferred_size.csv", ["time", "cumulative_size_mb"], [[0, 0], [30, 100 * scale]])
    write_csv(csv_dir / "file_created_size.csv", ["time", "cumulative_size_mb"], [[0, 0], [30, 50 * scale]])
    write_csv(csv_dir / "file_sizes.csv", ["file_idx", "file_name", "file_size_kb"], [[1, "input-a", 10 * scale], [2, "output-b", 30 * scale], [3, "output-c", 60 * scale]])
    write_csv(csv_dir / "file_concurrent_replicas.csv", ["file_idx", "file_name", "max_simul_replicas"], [[1, "input-a", 1], [2, "output-b", 2], [3, "output-c", 3]])
    write_csv(csv_dir / "file_retention_time.csv", ["file_idx", "file_name", "retention_time"], [[1, "input-a", 4], [2, "output-b", 8], [3, "output-c", 12]])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9137)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="vine-report-e2e-") as directory:
        root = Path(directory)
        make_run(root, "run-a", 1)
        make_run(root, "run-b", 2)
        app = create_app(str(root), request_threads=4)
        server = BoundedThreadPoolWSGIServer("127.0.0.1", args.port, app, max_workers=4)
        try:
            server.serve_forever()
        finally:
            server.server_close()


if __name__ == "__main__":
    main()
