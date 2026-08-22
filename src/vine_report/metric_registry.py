"""Single declarative registry for metric semantics and presentation."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class MetricSpec:
    id: str
    title: str
    description: str
    group: str
    renderer: str
    artifact: str
    x_field: str = "time"
    y_field: str | None = None
    mode: str = "single"
    x_unit: str = "count"
    y_unit: str = "count"
    curve: str = "linear"
    time_linked: bool = False
    time_origin: str = "debug"
    temporal_policy: str | None = None
    visible: bool = True
    source: str = "debug"
    inclusion: str = "observed records"
    exclusions: str = "missing values"
    semantic_version: int = 1
    series_columns: tuple[tuple[str, str], ...] = ()
    excluded_columns: tuple[str, ...] = ()
    extra_fields: tuple[str, ...] = ()
    lookup_field: str | None = None
    value_scale: float = 1.0
    quality_policy: str = "show_partial"
    options: dict[str, Any] = field(default_factory=dict)

    def catalog_entry(self) -> dict[str, Any]:
        item = asdict(self)
        for key in (
            "artifact", "x_field", "y_field", "series_columns",
            "excluded_columns", "extra_fields", "lookup_field", "value_scale",
        ):
            item.pop(key, None)
        return item


def _m(id, title, description, group, renderer, artifact, **kwargs):
    if kwargs.get("mode") == "source":
        kwargs.setdefault("time_origin", "manager")
    return MetricSpec(id, title, description, group, renderer, artifact, **kwargs)


_SPECS = [
    _m("task-execution-details", "Task Execution Timeline",
       "Places every recorded task attempt on its worker and time axis. Use it to find idle gaps, long-running work, and retries.",
       "tasks", "task_timeline", "task_execution_details.csv", mode="custom", x_unit="seconds",
       y_unit="worker_core", time_linked=True),
    _m("task-throughput", "Application Throughput",
       "Shows how quickly application task attempts finish. Compare successful and failed rates to identify slowdowns or bursts of failure.",
       "tasks", "line", "metric_task_throughput.csv", mode="source", x_unit="seconds",
       y_unit="attempts_per_second", curve="step", time_linked=True,
       temporal_policy="bucket_zero",
       source="transactions+debug-classification",
       series_columns=(("Successful", "success_rate_per_second"), ("Failed", "failure_rate_per_second")),
       options={"x_label": "Runtime", "y_label": "Attempts / second"}),
    _m("task-latency", "Task Latency Distribution",
       "Compares the queue, execution, and retrieval time distributions. Curves farther to the left indicate lower latency.",
       "tasks", "cdf", "metric_task_latency.csv", mode="source", x_unit="seconds", y_unit="ratio",
       source="transactions+debug-classification",
       series_columns=(("Queue", "queue_seconds"), ("Execution", "execution_seconds"), ("Retrieval", "retrieval_seconds")),
       options={"x_label": "Duration", "y_label": "Percentile"}),
    _m("failure-timeline", "Failure Timeline",
       "Groups failed application attempts by outcome over the run, making failure clusters and recurring causes easier to spot.",
       "tasks", "grouped", "metric_failure_timeline.csv", mode="source", x_unit="seconds", y_unit="count",
       curve="step", time_linked=True, source="transactions+debug-classification",
       temporal_policy="bucket_zero",
       options={"group": "status", "value": "count", "x_label": "Runtime", "y_label": "Failed attempts"}),
    _m("task-concurrency", "Task Concurrency",
       "Tracks how many tasks occupy each lifecycle state as the workflow progresses.",
       "tasks", "line", "task_concurrency.csv", mode="columns", x_unit="seconds", y_unit="count",
       curve="step", time_linked=True, temporal_policy="state", visible=False,
       series_columns=(("Waiting", "Waiting"), ("Committing", "Committing"), ("Executing", "Executing"),
                       ("Retrieving", "Retrieving"), ("Done", "Done"))),
    _m("task-response-time", "Task Response Time",
       "Shows the recorded delay from a task becoming ready until it runs or fails before dispatch.",
       "tasks", "scatter", "task_response_time.csv", x_field="Global Index", y_field="Response Time",
       mode="grouped_points", x_unit="count", y_unit="seconds",
       extra_fields=("Task ID", "Task Try ID", "Was Dispatched"),
       options={"group_index": 4, "groups": {"1": "Dispatched", "0": "Undispatched"}}),
    _m("task-execution-time", "Task Execution Time",
       "Shows the time each task attempt spent executing, helping reveal outliers and uneven workloads.",
       "tasks", "scatter", "task_execution_time.csv", x_field="Global Index", y_field="Execution Time",
       mode="grouped_points", x_unit="count", y_unit="seconds",
       extra_fields=("Task ID", "Task Try ID", "Ran to Completion"),
       options={"group_index": 4, "groups": {"1": "Ran to Completion", "0": "Failed"}}),
    _m("task-retrieval-time", "Task Retrieval Time",
       "Shows how long completed task outputs remained in retrieval before reaching the manager.",
       "tasks", "scatter", "task_retrieval_time.csv", x_field="Global Index", y_field="Retrieval Time",
       mode="points", x_unit="count", y_unit="seconds", extra_fields=("Task ID", "Task Try ID")),
    _m("task-completion-percentiles", "Task Completion Percentiles",
       "Shows when successive percentages of tasks completed relative to the beginning of the run.",
       "tasks", "line_points", "task_completion_percentiles.csv", x_field="Completion Time", y_field="Percentile",
       x_unit="seconds", y_unit="percent", time_linked=True, temporal_policy="state", visible=False),
    _m("task-dependencies", "Task Dependencies",
       "Shows how many upstream producer tasks each task depends on through its input files.",
       "tasks", "scatter", "task_dependencies.csv", x_field="Global Index", y_field="Dependency Count",
       mode="points", x_unit="count", y_unit="count", visible=False),
    _m("task-dependents", "Task Dependents",
       "Shows how many downstream tasks consume each task's output files.",
       "tasks", "scatter", "task_dependents.csv", x_field="Global Index", y_field="Dependent Count",
       mode="points", x_unit="count", y_unit="count", visible=False),
    _m("task-subgraphs", "Task Subgraphs",
       "Summarizes connected task groups inferred from input and output relationships.",
       "tasks", "task_subgraphs", "task_subgraphs.csv", mode="custom", visible=False),
    _m("worker-concurrency", "Worker Concurrency",
       "Tracks the number of connected workers over time, revealing scale-up, scale-down, and availability gaps.",
       "workers", "line", "worker_concurrency.csv", y_field="Active Workers (count)", x_unit="seconds",
       y_unit="count", curve="step", time_linked=True, temporal_policy="state"),
    _m("worker-capacity", "Worker Capacity",
       "Compares available cores, committed cores, and running tasks over time. Committed cores represent scheduler occupancy, not CPU utilization.",
       "workers", "line", "metric_worker_capacity.csv", mode="source", x_unit="seconds", y_unit="count",
       curve="step", time_linked=True, source="performance",
       temporal_policy="state",
       series_columns=(("Total cores", "total_cores"), ("Committed cores", "committed_cores"),
                       ("Running tasks", "tasks_running")),
       options={"x_label": "Runtime", "y_label": "Count"}),
    _m("worker-churn", "Worker Churn",
       "Tracks active workers as connections open and close, making unstable worker populations visible.",
       "workers", "line", "metric_worker_churn.csv", mode="source", x_unit="seconds", y_unit="count",
       curve="step", time_linked=True, temporal_policy="state", source="transactions", series_columns=(("Active workers", "active"),),
       options={"x_label": "Runtime", "y_label": "Workers"}),
    _m("worker-storage-consumption", "Worker Cache Occupancy",
       "Shows TaskVine-managed cache payload retained on workers. It does not represent total physical disk utilization.",
       "workers", "line", "metric_worker_cache_occupancy.csv", mode="cache_occupancy", x_unit="seconds",
       y_unit="bytes", curve="step", time_linked=True,
       temporal_policy="state",
       source="debug-cache-lifecycle+performance-reconciliation",
       inclusion="cache-update to cache-invalid/unlink residencies",
       exclusions="sandbox,temp,logs,filesystem-overhead",
       options={"views": ["total", "by-worker"], "default_view": "total", "default_series_limit": 24}),
    _m("worker-executing-tasks", "Executing Tasks by Worker",
       "Shows how many tasks each worker executes over time, revealing load balance and idle capacity.",
       "workers", "line", "worker_executing_tasks.csv", mode="wide", x_unit="seconds", y_unit="count",
       curve="step", time_linked=True, temporal_policy="state", excluded_columns=("workflow_completion_percentage",),
       options={"default_series_limit": 24}),
    _m("worker-waiting-retrieval-tasks", "Waiting Retrieval by Worker",
       "Shows completed tasks waiting for output retrieval on each worker.",
       "workers", "line", "worker_waiting_retrieval_tasks.csv", mode="wide", x_unit="seconds", y_unit="count",
       curve="step", time_linked=True, temporal_policy="state", visible=False, excluded_columns=("workflow_completion_percentage",),
       options={"default_series_limit": 24}),
    _m("worker-lifetime", "Worker Lifetime",
       "Compares how long workers remained connected to the manager during the run.",
       "workers", "scatter", "worker_lifetime.csv", x_field="ID", y_field="LifeTime (s)", mode="points",
       x_unit="count", y_unit="seconds", lookup_field="Worker IP Port"),
    _m("worker-incoming-transfers", "Incoming Transfers",
       "Shows concurrent transfers arriving at each worker, helping identify receivers under sustained transfer pressure.",
       "transfers", "line", "worker_incoming_transfers.csv", mode="wide", x_unit="seconds", y_unit="count",
       curve="step", time_linked=True, temporal_policy="state", excluded_columns=("workflow_completion_percentage",),
       options={"default_series_limit": 24}),
    _m("worker-outgoing-transfers", "Outgoing Transfers",
       "Shows concurrent transfers leaving each worker, helping identify heavily used data sources.",
       "transfers", "line", "worker_outgoing_transfers.csv", mode="wide", x_unit="seconds", y_unit="count",
       curve="step", time_linked=True, temporal_policy="state", excluded_columns=("workflow_completion_percentage",),
       options={"default_series_limit": 24}),
    _m("transfer-throughput", "Transfer Throughput",
       "Shows completed input and output payload bytes per second. This is payload throughput, not physical network utilization.",
       "transfers", "line", "metric_transfer_throughput.csv", mode="source", x_unit="seconds",
       y_unit="bytes_per_second", curve="step", time_linked=True, source="transactions",
       temporal_policy="bucket_zero",
       series_columns=(("Input", "input_bytes_per_second"), ("Output", "output_bytes_per_second")),
       options={"x_label": "Runtime", "y_label": "Payload bytes / second"}),
    _m("file-sizes", "File Sizes",
       "Compares the sizes of files observed in the workflow and highlights unusually large data objects.",
       "files", "scatter", "file_sizes.csv", x_field="file_idx", mode="points", x_unit="count", y_unit="bytes",
       lookup_field="file_name", options={"value_fields": {"file_size_mb": 2**20, "file_size_kb": 2**10}}),
    _m("file-concurrent-replicas", "Concurrent Replicas",
       "Shows the maximum number of simultaneous replicas observed for each file.",
       "files", "scatter", "file_concurrent_replicas.csv", x_field="file_idx", y_field="max_simul_replicas",
       mode="points", x_unit="count", y_unit="count", lookup_field="file_name"),
    _m("file-retention-time", "File Retention Time",
       "Shows how long each file remained present across worker transfers, from first stage-in to last stage-out.",
       "files", "scatter", "file_retention_time.csv", x_field="file_idx", y_field="retention_time",
       mode="points", x_unit="count", y_unit="seconds", lookup_field="file_name"),
    _m("file-transferred-size", "Cumulative Data Transferred",
       "Tracks the cumulative volume of file data transferred as the workflow progresses.",
       "files", "line", "file_transferred_size.csv", y_field="cumulative_size_mb", x_unit="seconds",
       y_unit="bytes", curve="step", time_linked=True, temporal_policy="state", value_scale=2**20),
    _m("file-created-size", "Cumulative Data Created",
       "Tracks the cumulative size of files first observed or produced during the run.",
       "files", "line", "file_created_size.csv", y_field="cumulative_size_mb", x_unit="seconds",
       y_unit="bytes", curve="step", time_linked=True, temporal_policy="state", value_scale=2**20),
]

METRIC_SPECS = {spec.id: spec for spec in _SPECS}


def metric_spec(metric_id: str) -> MetricSpec | None:
    return METRIC_SPECS.get(metric_id)


def metric_catalog() -> list[dict[str, Any]]:
    return [spec.catalog_entry() for spec in METRIC_SPECS.values()]


def standard_metric_ids() -> tuple[str, ...]:
    return tuple(spec.id for spec in _SPECS if spec.mode not in {"source", "custom"})
