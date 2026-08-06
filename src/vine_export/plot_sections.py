"""
Plot implementations for vine_export sections (except task_execution_details).
"""

import pandas as pd

from src.utils import get_size_unit_and_scale, get_time_domain_from_csv
from src.vine_export.config import (
    DPI_DEFAULT,
    DEFAULT_LINEAR_TICK_COUNT,
    MARKER_SIZE,
    LINE_WIDTH,
    LEGEND_FONT_SIZE,
    MAX_MULTI_SERIES_LINES,
    MULTI_SERIES_LINE_ALPHA,
    MULTI_SERIES_LINE_WIDTH,
    COLOR_SUCCESS,
    COLOR_FAILURE,
    COLOR_NEUTRAL,
    COLOR_WAITING,
    COLOR_COMMITTING,
    COLOR_EXECUTING,
    COLOR_RETRIEVING,
    COLOR_DONE,
)
from src.vine_export.plot_common import make_figure, style_axes, set_linear_ticks, load_csv, save_plot


def _valid_xy(df, x_col, y_col):
    x = pd.to_numeric(df[x_col], errors="coerce")
    y = pd.to_numeric(df[y_col], errors="coerce")
    valid = x.notna() & y.notna()
    return x[valid], y[valid]


def _safe_domain(min_v, max_v, pad=0.0):
    if pd.isna(min_v) or pd.isna(max_v):
        return [0.0, 1.0]
    min_v = float(min_v)
    max_v = float(max_v)
    if max_v <= min_v:
        max_v = min_v + 1.0
    if pad > 0:
        span = max_v - min_v
        min_v -= span * pad
        max_v += span * pad
    return [min_v, max_v]


def _time_domain_or_fallback(csv_files_dir, x_series):
    domain = get_time_domain_from_csv(csv_files_dir)
    if domain is not None:
        return [float(domain[0]), float(domain[1])]
    return _safe_domain(float(x_series.min()), float(x_series.max()))


def _plot_scatter_basic(
    csv_files_dir,
    png_files_dir,
    csv_name,
    png_name,
    title,
    x_col,
    y_col,
    x_label,
    y_label,
    dpi=DPI_DEFAULT,
    width=None,
    height=None,
    color=COLOR_SUCCESS,
):
    import matplotlib.pyplot as plt

    df = load_csv(csv_files_dir, csv_name)
    x, y = _valid_xy(df, x_col, y_col)
    if len(x) == 0:
        return None

    fig, ax = make_figure(plt, width=width, height=height)
    ax.scatter(x, y, s=MARKER_SIZE, alpha=0.65, c=color, edgecolors="none")
    ax.set_xlim(*_safe_domain(x.min(), x.max()))
    ax.set_ylim(*_safe_domain(min(0.0, y.min()), y.max()))
    style_axes(ax, title, x_label, y_label)
    set_linear_ticks(ax, [x.min(), x.max()], axis="x", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=2)
    set_linear_ticks(ax, [min(0.0, y.min()), y.max()], axis="y", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=2)
    return save_plot(fig, plt, png_files_dir, png_name, dpi)


def _plot_multi_series_time(
    csv_files_dir,
    png_files_dir,
    csv_name,
    png_name,
    title,
    y_label,
    dpi=DPI_DEFAULT,
    width=None,
    height=None,
    exclude_cols=None,
    force_colors=None,
):
    import matplotlib.pyplot as plt

    df = load_csv(csv_files_dir, csv_name)
    if "time" not in df.columns:
        return None
    x = pd.to_numeric(df["time"], errors="coerce")
    valid_time = x.notna()
    if valid_time.sum() == 0:
        return None

    exclude_cols = set(exclude_cols or [])
    exclude_cols.add("time")
    series_cols = [c for c in df.columns if c not in exclude_cols]
    if not series_cols:
        return None

    ranked = []
    for col in series_cols:
        y = pd.to_numeric(df[col], errors="coerce")
        valid_count = int(y.notna().sum())
        vmax = y.max(skipna=True)
        if valid_count > 0 and pd.notna(vmax) and vmax > 0:
            ranked.append((col, valid_count, float(vmax)))
    if not ranked:
        return None

    # Priority rule:
    # 1) series with more valid data points
    # 2) then higher peak value
    ranked.sort(key=lambda kv: (kv[1], kv[2]), reverse=True)

    # Always keep at least one line that contains the global maximum value.
    global_max = max(v for _, _, v in ranked)
    must_keep = {c for c, _, v in ranked if v == global_max}

    if len(ranked) > MAX_MULTI_SERIES_LINES:
        selected = []
        for c, _, _ in ranked:
            if c in must_keep and c not in selected:
                selected.append(c)
        for c, _, _ in ranked:
            if len(selected) >= MAX_MULTI_SERIES_LINES:
                break
            if c not in selected:
                selected.append(c)
        selected_cols = selected[:MAX_MULTI_SERIES_LINES]
    else:
        selected_cols = [c for c, _, _ in ranked]

    fig, ax = make_figure(plt, width=width, height=height)
    ymax = 0.0
    for col in selected_cols:
        y = pd.to_numeric(df[col], errors="coerce")
        valid = valid_time & y.notna()
        if valid.sum() == 0:
            continue
        ymax = max(ymax, float(y[valid].max()))
        color = None
        alpha = MULTI_SERIES_LINE_ALPHA
        lw = MULTI_SERIES_LINE_WIDTH
        if force_colors and col in force_colors:
            color = force_colors[col]
            alpha = 0.95
            lw = LINE_WIDTH
        ax.plot(x[valid], y[valid], color=color, alpha=alpha, linewidth=lw, drawstyle="steps-post")

    if ymax <= 0:
        ymax = 1.0
    x_domain = _time_domain_or_fallback(csv_files_dir, x[valid_time])
    y_domain = [0.0, ymax]
    ax.set_xlim(*x_domain)
    ax.set_ylim(*y_domain)
    style_axes(ax, title, "Time (s)", y_label)
    set_linear_ticks(ax, x_domain, axis="x", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=2, suffix=" s")
    set_linear_ticks(ax, y_domain, axis="y", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=2)

    if len(ranked) > len(selected_cols):
        ax.text(
            0.99,
            0.99,
            f"Showing top {len(selected_cols)} of {len(ranked)} series",
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=max(8, LEGEND_FONT_SIZE - 4),
            color=COLOR_NEUTRAL,
        )
    return save_plot(fig, plt, png_files_dir, png_name, dpi)


def plot_task_concurrency(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    import matplotlib.pyplot as plt

    df = load_csv(csv_files_dir, "task_concurrency.csv")
    if "time" not in df.columns:
        return None
    x = pd.to_numeric(df["time"], errors="coerce")
    phases = [
        ("Waiting", COLOR_WAITING),
        ("Committing", COLOR_COMMITTING),
        ("Executing", COLOR_EXECUTING),
        ("Retrieving", COLOR_RETRIEVING),
        ("Done", COLOR_DONE),
    ]

    fig, ax = make_figure(plt, width=width, height=height)
    ymax = 0.0
    for col, color in phases:
        if col not in df.columns:
            continue
        y = pd.to_numeric(df[col], errors="coerce")
        valid = x.notna() & y.notna()
        if valid.sum() == 0:
            continue
        ymax = max(ymax, float(y[valid].max()))
        ax.plot(x[valid], y[valid], linewidth=LINE_WIDTH, color=color, label=col, alpha=0.95, drawstyle="steps-post")

    if ymax <= 0:
        ymax = 1.0
    x_domain = _time_domain_or_fallback(csv_files_dir, x.dropna())
    y_domain = [0.0, ymax]
    ax.set_xlim(*x_domain)
    ax.set_ylim(*y_domain)
    style_axes(ax, "Task Concurrency", "Time (s)", "Task Count")
    set_linear_ticks(ax, x_domain, axis="x", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=2, suffix=" s")
    set_linear_ticks(ax, y_domain, axis="y", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=0)
    ax.legend(loc="upper left", fontsize=LEGEND_FONT_SIZE, frameon=False)
    return save_plot(fig, plt, png_files_dir, "task-concurrency.png", dpi)


def plot_task_response_time(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    import matplotlib.pyplot as plt

    df = load_csv(csv_files_dir, "task_response_time.csv")
    x = pd.to_numeric(df["Global Index"], errors="coerce")
    y = pd.to_numeric(df["Response Time"], errors="coerce")
    dispatch = df["Was Dispatched"].astype(str).str.lower().isin(["true", "1", "yes"])
    valid = x.notna() & y.notna()
    if valid.sum() == 0:
        return None

    fig, ax = make_figure(plt, width=width, height=height)
    mask_ok = valid & dispatch
    mask_bad = valid & (~dispatch)
    ax.scatter(x[mask_ok], y[mask_ok], s=MARKER_SIZE, c=COLOR_SUCCESS, alpha=0.65, label="Dispatched", edgecolors="none")
    ax.scatter(x[mask_bad], y[mask_bad], s=MARKER_SIZE, c=COLOR_FAILURE, alpha=0.65, label="Undispatched", edgecolors="none")
    x_domain = _safe_domain(x[valid].min(), x[valid].max())
    y_domain = _safe_domain(min(0.0, y[valid].min()), y[valid].max())
    ax.set_xlim(*x_domain)
    ax.set_ylim(*y_domain)
    style_axes(ax, "Task Response Time", "Global Index", "Response Time (s)")
    set_linear_ticks(ax, x_domain, axis="x", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=0)
    set_linear_ticks(ax, y_domain, axis="y", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=2)
    ax.legend(loc="upper left", fontsize=LEGEND_FONT_SIZE, frameon=False)
    return save_plot(fig, plt, png_files_dir, "task-response-time.png", dpi)


def plot_task_execution_time(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    import matplotlib.pyplot as plt

    df = load_csv(csv_files_dir, "task_execution_time.csv")
    x = pd.to_numeric(df["Global Index"], errors="coerce")
    y = pd.to_numeric(df["Execution Time"], errors="coerce")
    success = df["Ran to Completion"].astype(str).str.lower().isin(["true", "1", "yes"])
    valid = x.notna() & y.notna()
    if valid.sum() == 0:
        return None

    fig, ax = make_figure(plt, width=width, height=height)
    mask_ok = valid & success
    mask_bad = valid & (~success)
    ax.scatter(x[mask_ok], y[mask_ok], s=MARKER_SIZE, c=COLOR_SUCCESS, alpha=0.65, label="Ran to completion", edgecolors="none")
    ax.scatter(x[mask_bad], y[mask_bad], s=MARKER_SIZE, c=COLOR_FAILURE, alpha=0.65, label="Failed", edgecolors="none")
    x_domain = _safe_domain(x[valid].min(), x[valid].max())
    y_domain = _safe_domain(min(0.0, y[valid].min()), y[valid].max())
    ax.set_xlim(*x_domain)
    ax.set_ylim(*y_domain)
    style_axes(ax, "Task Execution Time", "Global Index", "Execution Time (s)")
    set_linear_ticks(ax, x_domain, axis="x", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=0)
    set_linear_ticks(ax, y_domain, axis="y", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=2)
    ax.legend(loc="upper left", fontsize=LEGEND_FONT_SIZE, frameon=False)
    return save_plot(fig, plt, png_files_dir, "task-execution-time.png", dpi)


def plot_task_retrieval_time(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    return _plot_scatter_basic(
        csv_files_dir,
        png_files_dir,
        csv_name="task_retrieval_time.csv",
        png_name="task-retrieval-time.png",
        title="Task Retrieval Time",
        x_col="Global Index",
        y_col="Retrieval Time",
        x_label="Global Index",
        y_label="Retrieval Time (s)",
        dpi=dpi,
        width=width,
        height=height,
        color=COLOR_EXECUTING,
    )


def plot_worker_concurrency(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    import matplotlib.pyplot as plt

    df = load_csv(csv_files_dir, "worker_concurrency.csv")
    x, y = _valid_xy(df, "time", "Active Workers (count)")
    if len(x) == 0:
        return None
    fig, ax = make_figure(plt, width=width, height=height)
    ax.plot(x, y, linewidth=LINE_WIDTH, color=COLOR_EXECUTING, drawstyle="steps-post")
    x_domain = _time_domain_or_fallback(csv_files_dir, x)
    y_domain = _safe_domain(0.0, y.max())
    ax.set_xlim(*x_domain)
    ax.set_ylim(*y_domain)
    style_axes(ax, "Worker Concurrency", "Time (s)", "Active Workers")
    set_linear_ticks(ax, x_domain, axis="x", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=2, suffix=" s")
    set_linear_ticks(ax, y_domain, axis="y", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=0)
    return save_plot(fig, plt, png_files_dir, "worker-concurrency.png", dpi)


def plot_worker_storage_consumption(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    return _plot_multi_series_time(
        csv_files_dir,
        png_files_dir,
        csv_name="worker_storage_consumption.csv",
        png_name="worker-storage-consumption.png",
        title="Worker Storage Consumption",
        y_label="Storage (MB)",
        dpi=dpi,
        width=width,
        height=height,
        exclude_cols=["workflow_completion_percentage"],
    )


def plot_worker_incoming_transfers(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    return _plot_multi_series_time(
        csv_files_dir,
        png_files_dir,
        csv_name="worker_incoming_transfers.csv",
        png_name="worker-incoming-transfers.png",
        title="Worker Incoming Transfers",
        y_label="Incoming Transfer Count",
        dpi=dpi,
        width=width,
        height=height,
    )


def plot_worker_outgoing_transfers(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    return _plot_multi_series_time(
        csv_files_dir,
        png_files_dir,
        csv_name="worker_outgoing_transfers.csv",
        png_name="worker-outgoing-transfers.png",
        title="Worker Outgoing Transfers",
        y_label="Outgoing Transfer Count",
        dpi=dpi,
        width=width,
        height=height,
    )


def plot_worker_executing_tasks(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    return _plot_multi_series_time(
        csv_files_dir,
        png_files_dir,
        csv_name="worker_executing_tasks.csv",
        png_name="worker-executing-tasks.png",
        title="Worker Executing Tasks",
        y_label="Executing Task Count",
        dpi=dpi,
        width=width,
        height=height,
    )


def plot_worker_lifetime(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    return _plot_scatter_basic(
        csv_files_dir,
        png_files_dir,
        csv_name="worker_lifetime.csv",
        png_name="worker-lifetime.png",
        title="Worker Lifetime",
        x_col="ID",
        y_col="LifeTime (s)",
        x_label="Worker ID",
        y_label="Lifetime (s)",
        dpi=dpi,
        width=width,
        height=height,
        color=COLOR_EXECUTING,
    )


def plot_file_sizes(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    import matplotlib.pyplot as plt

    df = load_csv(csv_files_dir, "file_sizes.csv")
    x = pd.to_numeric(df["file_idx"], errors="coerce")
    y_mb = pd.to_numeric(df["file_size_mb"], errors="coerce")
    valid = x.notna() & y_mb.notna()
    if valid.sum() == 0:
        return None
    unit, scale = get_size_unit_and_scale(float(y_mb[valid].max()))
    y = y_mb * scale

    fig, ax = make_figure(plt, width=width, height=height)
    ax.scatter(x[valid], y[valid], s=MARKER_SIZE, alpha=0.65, c=COLOR_EXECUTING, edgecolors="none")
    x_domain = _safe_domain(x[valid].min(), x[valid].max())
    y_domain = _safe_domain(0.0, y[valid].max())
    ax.set_xlim(*x_domain)
    ax.set_ylim(*y_domain)
    style_axes(ax, "File Sizes", "File Index", f"File Size ({unit})")
    set_linear_ticks(ax, x_domain, axis="x", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=0)
    set_linear_ticks(ax, y_domain, axis="y", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=2)
    return save_plot(fig, plt, png_files_dir, "file-sizes.png", dpi)


def plot_file_concurrent_replicas(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    return _plot_scatter_basic(
        csv_files_dir,
        png_files_dir,
        csv_name="file_concurrent_replicas.csv",
        png_name="file-concurrent-replicas.png",
        title="File Concurrent Replicas",
        x_col="file_idx",
        y_col="max_simul_replicas",
        x_label="File Index",
        y_label="Max Concurrent Replicas",
        dpi=dpi,
        width=width,
        height=height,
        color=COLOR_EXECUTING,
    )


def plot_file_retention_time(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    return _plot_scatter_basic(
        csv_files_dir,
        png_files_dir,
        csv_name="file_retention_time.csv",
        png_name="file-retention-time.png",
        title="File Retention Time",
        x_col="file_idx",
        y_col="retention_time",
        x_label="File Index",
        y_label="Retention Time (s)",
        dpi=dpi,
        width=width,
        height=height,
        color=COLOR_EXECUTING,
    )


def _plot_cumulative_size_line(
    csv_files_dir,
    png_files_dir,
    csv_name,
    png_name,
    title,
    dpi=DPI_DEFAULT,
    width=None,
    height=None,
):
    import matplotlib.pyplot as plt

    df = load_csv(csv_files_dir, csv_name)
    if "time" not in df.columns:
        return None
    x = pd.to_numeric(df["time"], errors="coerce")
    if "cumulative_size_mb" in df.columns:
        y_mb = pd.to_numeric(df["cumulative_size_mb"], errors="coerce")
    elif "delta_size_mb" in df.columns:
        y_mb = pd.to_numeric(df["delta_size_mb"], errors="coerce").fillna(0).cumsum()
    else:
        return None
    valid = x.notna() & y_mb.notna()
    if valid.sum() == 0:
        return None

    unit, scale = get_size_unit_and_scale(float(y_mb[valid].max()))
    y = y_mb * scale

    fig, ax = make_figure(plt, width=width, height=height)
    ax.plot(x[valid], y[valid], linewidth=LINE_WIDTH, color=COLOR_EXECUTING, drawstyle="steps-post")
    x_domain = _time_domain_or_fallback(csv_files_dir, x[valid])
    y_domain = _safe_domain(0.0, y[valid].max())
    ax.set_xlim(*x_domain)
    ax.set_ylim(*y_domain)
    style_axes(ax, title, "Time (s)", f"Cumulative Size ({unit})")
    set_linear_ticks(ax, x_domain, axis="x", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=2, suffix=" s")
    set_linear_ticks(ax, y_domain, axis="y", tick_count=DEFAULT_LINEAR_TICK_COUNT, round_digits=2)
    return save_plot(fig, plt, png_files_dir, png_name, dpi)


def plot_file_transferred_size(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    return _plot_cumulative_size_line(
        csv_files_dir,
        png_files_dir,
        csv_name="file_transferred_size.csv",
        png_name="file-transferred-size.png",
        title="File Transferred Size",
        dpi=dpi,
        width=width,
        height=height,
    )


def plot_file_created_size(csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, width=None, height=None):
    return _plot_cumulative_size_line(
        csv_files_dir,
        png_files_dir,
        csv_name="file_created_size.csv",
        png_name="file-created-size.png",
        title="File Created Size",
        dpi=dpi,
        width=width,
        height=height,
    )

