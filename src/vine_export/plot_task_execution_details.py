"""
Plot Task Execution Details section for vine_export.

Reads task_execution_details.csv, draws worker/task bars with matplotlib.
Same data processing as vine_report (shared utils, config).
"""

import os
import pandas as pd

from src.utils import (
    read_csv_to_fd,
    ensure_dir,
    task_execution_safe_int,
    task_execution_safe_float,
    parse_worker_time_list,
    get_time_domain_from_csv,
    compute_task_execution_x_domain,
    compute_linear_tick_values,
)
from src.vine_export.config import (
    CANVAS_HEIGHT_INCHES,
    CANVAS_WIDTH_INCHES,
    DPI_DEFAULT,
    MAX_TASKS_PER_TYPE,
    LAYOUT_PAD,
    LAYOUT_W_PAD,
    LAYOUT_H_PAD,
    TITLE_SIZE,
    TITLE_PAD,
    LABEL_SIZE,
    X_LABEL_PAD,
    Y_LABEL_PAD,
    TICK_LABEL_SIZE,
    LEGEND_FONT_SIZE,
    downsample_tasks,
)

# Colors aligned with vine_report LEGEND_SCHEMA
WORKER_COLOR = "lightgrey"
WORKER_ALPHA = 0.3
PHASE_COLORS = {
    "successful-committing-to-worker": "#0ecfc8",
    "successful-executing-on-worker": "steelblue",
    "successful-retrieving-to-manager": "#cc5a12",
    "recovery-successful": "#FF69B4",
    "recovery-unsuccessful": "#E3314F",
}
UNSUCCESSFUL_COLORS = {
    "unsuccessful-input-missing": "#FFB6C1",
    "unsuccessful-output-missing": "#FF69B4",
    "unsuccessful-stdout-missing": "#FF1493",
    "unsuccessful-signal": "#CD5C5C",
    "unsuccessful-resource-exhaustion": "#8B0000",
    "unsuccessful-max-end-time": "#B22222",
    "unsuccessful-unknown": "#A52A2A",
    "unsuccessful-forsaken": "#E331EE",
    "unsuccessful-max-retries": "#8B4513",
    "unsuccessful-max-wall-time": "#D2691E",
    "unsuccessful-monitor-error": "#FF4444",
    "unsuccessful-output-transfer-error": "#FF6B6B",
    "unsuccessful-location-missing": "#FF8787",
    "unsuccessful-cancelled": "#FFA07A",
    "unsuccessful-library-exit": "#FA8072",
    "unsuccessful-sandbox-exhaustion": "#E9967A",
    "unsuccessful-missing-library": "#F08080",
    "unsuccessful-worker-disconnected": "#FF0000",
    "unsuccessful-undispatched": "#9370DB",
    "unsuccessful-failed-to-dispatch": "#8A2BE2",
}
DEFAULT_UNSUCCESSFUL_COLOR = "#A52A2A"
UNSUCCESSFUL_LABELS = {
    "unsuccessful-input-missing": "Unsuccessful: Input Missing",
    "unsuccessful-output-missing": "Unsuccessful: Output Missing",
    "unsuccessful-stdout-missing": "Unsuccessful: Stdout Missing",
    "unsuccessful-signal": "Unsuccessful: Signal",
    "unsuccessful-resource-exhaustion": "Unsuccessful: Resource Exhaustion",
    "unsuccessful-max-end-time": "Unsuccessful: Max End Time",
    "unsuccessful-unknown": "Unsuccessful: Unknown",
    "unsuccessful-forsaken": "Unsuccessful: Forsaken",
    "unsuccessful-max-retries": "Unsuccessful: Max Retries",
    "unsuccessful-max-wall-time": "Unsuccessful: Max Wall Time",
    "unsuccessful-monitor-error": "Unsuccessful: Monitor Error",
    "unsuccessful-output-transfer-error": "Unsuccessful: Output Transfer Error",
    "unsuccessful-location-missing": "Unsuccessful: Location Missing",
    "unsuccessful-cancelled": "Unsuccessful: Cancelled",
    "unsuccessful-library-exit": "Unsuccessful: Library Exit",
    "unsuccessful-sandbox-exhaustion": "Unsuccessful: Sandbox Exhaustion",
    "unsuccessful-missing-library": "Unsuccessful: Missing Library",
    "unsuccessful-worker-disconnected": "Unsuccessful: Worker Disconnected",
    "unsuccessful-undispatched": "Unsuccessful: Undispatched",
    "unsuccessful-failed-to-dispatch": "Unsuccessful: Failed to Dispatch",
}


def load_task_execution_data(csv_files_dir):
    """Load and parse task_execution_details.csv and time_domain.csv."""
    csv_path = os.path.join(csv_files_dir, "task_execution_details.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"task_execution_details.csv not found: {csv_path}")

    df = read_csv_to_fd(csv_path)
    successful_tasks = []
    unsuccessful_tasks = []
    workers = []

    task_rows = df[df["record_type"].isin(["successful_tasks", "unsuccessful_tasks"])]
    for _, row in task_rows.iterrows():
        if pd.isna(row["task_id"]):
            continue

        base_task_data = {
            "task_id": int(row["task_id"]),
            "try_id": int(row["task_try_id"]),
            "worker_entry": str(row["worker_entry"]) if pd.notna(row["worker_entry"]) else "",
            "worker_id": task_execution_safe_int(row, "worker_id"),
            "core_id": task_execution_safe_int(row, "core_id"),
            "cores_requested": task_execution_safe_int(row, "cores_requested"),
            "is_recovery_task": bool(row["is_recovery_task"]) if pd.notna(row["is_recovery_task"]) else False,
        }

        if row["record_type"] == "successful_tasks":
            tws = task_execution_safe_float(row, "time_worker_start")
            twe = task_execution_safe_float(row, "time_worker_end")
            base_task_data.update(
                {
                    "when_running": task_execution_safe_float(row, "when_running"),
                    "time_worker_start": tws,
                    "time_worker_end": twe,
                    "when_waiting_retrieval": task_execution_safe_float(row, "when_waiting_retrieval"),
                    "when_retrieved": task_execution_safe_float(row, "when_retrieved"),
                    "execution_time": (twe - tws) if (tws is not None and twe is not None) else None,
                }
            )
            successful_tasks.append(base_task_data)
        else:
            wr = task_execution_safe_float(row, "when_running")
            wf = task_execution_safe_float(row, "when_failure_happens")
            base_task_data.update(
                {
                    "when_running": wr,
                    "when_failure_happens": wf,
                    "execution_time": (wf - wr) if (wr is not None and wf is not None) else None,
                    "unsuccessful_checkbox_name": str(row["unsuccessful_checkbox_name"])
                    if pd.notna(row["unsuccessful_checkbox_name"])
                    else "unsuccessful-unknown",
                }
            )
            unsuccessful_tasks.append(base_task_data)

    worker_rows = df[df["record_type"] == "worker"]
    for _, row in worker_rows.iterrows():
        if pd.isna(row["worker_id"]):
            continue

        time_connected = parse_worker_time_list(row["time_connected"])
        time_disconnected = parse_worker_time_list(row["time_disconnected"])

        workers.append(
            {
                "id": int(row["worker_id"]),
                "worker_entry": str(row["worker_entry"]) if pd.notna(row["worker_entry"]) else "",
                "time_connected": time_connected,
                "time_disconnected": time_disconnected,
                "cores": int(row["cores"]) if pd.notna(row["cores"]) else 1,
            }
        )

    # y_domain: worker_id-core_id (same as vine_report route)
    band_set = set()
    for w in workers:
        cores = int(w.get("cores", 1) or 1)
        for c in range(1, max(1, cores) + 1):
            band_set.add(f"{w['id']}-{c}")
    for t in successful_tasks + unsuccessful_tasks:
        wid = t.get("worker_id")
        cid = t.get("core_id")
        if wid is not None and cid is not None:
            band_set.add(f"{wid}-{cid}")
    y_domain = sorted(band_set, key=lambda k: (int(k.split("-")[0]), int(k.split("-")[1])))

    # x_domain: use time_domain.csv [0, MAX_TIME - MIN_TIME] to match vine_report
    x_domain = get_time_domain_from_csv(csv_files_dir)
    if x_domain is None:
        x_domain = compute_task_execution_x_domain(successful_tasks, unsuccessful_tasks)

    return {
        "successful_tasks": successful_tasks,
        "unsuccessful_tasks": unsuccessful_tasks,
        "workers": workers,
        "y_domain": y_domain,
        "x_domain": x_domain,
    }


def plot_task_execution_details(
    csv_files_dir, png_files_dir, dpi=DPI_DEFAULT, max_tasks=MAX_TASKS_PER_TYPE, width=None, height=None
):
    """
    Generate task-execution-details.png from CSV data.

    Args:
        csv_files_dir: Path to csv-files/
        png_files_dir: Path to png-files/
        dpi: DPI for PNG
        max_tasks: Max tasks per type to plot (downsample for large logs)
    """
    import matplotlib
    from src.vine_export.config import MATPLOTLIB_BACKEND

    matplotlib.use(MATPLOTLIB_BACKEND)
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle, Patch

    data = load_task_execution_data(csv_files_dir)
    successful_tasks = data["successful_tasks"]
    unsuccessful_tasks = data["unsuccessful_tasks"]
    workers = data["workers"]
    y_domain = data["y_domain"]
    x_domain = data["x_domain"]

    if not y_domain:
        return None

    successful_tasks = downsample_tasks(successful_tasks, max_tasks=max_tasks)
    unsuccessful_tasks = downsample_tasks(unsuccessful_tasks, max_tasks=max_tasks)

    # Keep x_domain from time_domain.csv (do NOT recompute from tasks)
    # Rebuild y_domain after downsampling (same strategy as vine_report)
    band_set = set()
    for w in workers:
        cores = int(w.get("cores", 1) or 1)
        for c in range(1, max(1, cores) + 1):
            band_set.add(f"{w['id']}-{c}")
    for t in successful_tasks + unsuccessful_tasks:
        wid = t.get("worker_id")
        cid = t.get("core_id")
        if wid is not None and cid is not None:
            band_set.add(f"{wid}-{cid}")
    y_domain = sorted(band_set, key=lambda k: (int(k.split("-")[0]), int(k.split("-")[1])))

    band_to_idx = {band: i for i, band in enumerate(y_domain)}
    x_min, x_max = float(x_domain[0]), float(x_domain[1])
    if x_max <= x_min:
        x_max = x_min + 1

    width = width if width is not None else CANVAS_WIDTH_INCHES
    height = height if height is not None else CANVAS_HEIGHT_INCHES
    fig, ax = plt.subplots(figsize=(width, height), constrained_layout=True)
    if hasattr(fig, "set_constrained_layout_pads"):
        fig.set_constrained_layout_pads(
            w_pad=LAYOUT_PAD,
            h_pad=LAYOUT_PAD,
            wspace=LAYOUT_W_PAD,
            hspace=LAYOUT_H_PAD,
        )
    else:
        fig.get_layout_engine().set(w_pad=LAYOUT_PAD, h_pad=LAYOUT_PAD)
    ax.set_xlim(x_min, x_max)
    ax.set_ylim(-0.5, len(y_domain) - 0.5)
    ax.set_aspect("auto")
    band_height = 0.8

    worker_patches = []
    task_patches = []
    task_colors = []
    seen_unsuccessful_types = set()
    has_recovery_successful = False
    has_recovery_unsuccessful = False

    # Workers (background): one rectangle spanning all worker cores.
    for w in workers:
        wid = w["id"]
        cores = int(w.get("cores", 1) or 1)
        first_band = f"{wid}-1"
        if first_band not in band_to_idx:
            continue
        y_idx_bottom = band_to_idx[first_band]
        y_bottom = y_idx_bottom - band_height / 2
        worker_height = (band_height * max(1, cores)) + ((1.0 - band_height) * (max(1, cores) - 1))

        for i in range(len(w["time_connected"])):
            t_start = w["time_connected"][i]
            t_end = (
                w["time_disconnected"][i]
                if i < len(w["time_disconnected"])
                else x_max
            )
            if t_start is None or t_end is None:
                continue
            ww = max(0.0, float(t_end) - float(t_start))
            if ww > 0:
                worker_patches.append(
                    Rectangle((float(t_start), float(y_bottom)), ww, worker_height, transform=ax.transData)
                )

    # Unsuccessful tasks
    for t in unsuccessful_tasks:
        wid = t.get("worker_id")
        cid = t.get("core_id")
        band = f"{wid}-{cid}" if (wid is not None and cid is not None) else None
        if band is None or band not in band_to_idx:
            continue
        y_idx = band_to_idx[band]
        t_start = t.get("when_running")
        t_end = t.get("when_failure_happens")
        if t_start is None or t_end is None or t_end <= t_start:
            continue
        color = UNSUCCESSFUL_COLORS.get(
            t.get("unsuccessful_checkbox_name", "unsuccessful-unknown"),
            DEFAULT_UNSUCCESSFUL_COLOR,
        )
        unsuccessful_type = t.get("unsuccessful_checkbox_name", "unsuccessful-unknown")
        if t.get("is_recovery_task"):
            color = PHASE_COLORS.get("recovery-unsuccessful", color)
            has_recovery_unsuccessful = True
        else:
            seen_unsuccessful_types.add(unsuccessful_type)
        ww = max(0.0, float(t_end) - float(t_start))
        if ww > 0:
            y_bottom = y_idx - band_height / 2
            task_patches.append(
                Rectangle((float(t_start), float(y_bottom)), ww, band_height, transform=ax.transData)
            )
            task_colors.append(color)

    # Successful tasks (3 phases)
    phases = [
        ("when_running", "time_worker_start", "successful-committing-to-worker"),
        ("time_worker_start", "time_worker_end", "successful-executing-on-worker"),
        ("time_worker_end", "when_waiting_retrieval", "successful-retrieving-to-manager"),
    ]
    for t in successful_tasks:
        wid = t.get("worker_id")
        cid = t.get("core_id")
        band = f"{wid}-{cid}" if (wid is not None and cid is not None) else None
        if band is None or band not in band_to_idx:
            continue
        y_idx = band_to_idx[band]
        color_key = "recovery-successful" if t.get("is_recovery_task") else None
        if color_key:
            has_recovery_successful = True
        y_bottom = y_idx - band_height / 2

        for start_key, end_key, phase_name in phases:
            t_start = t.get(start_key)
            t_end = t.get(end_key)
            if t_start is None or t_end is None or t_end <= t_start:
                continue
            color = PHASE_COLORS.get(phase_name, "steelblue")
            if color_key:
                color = PHASE_COLORS.get(color_key, color)
            ww = max(0.0, float(t_end) - float(t_start))
            if ww > 0:
                task_patches.append(
                    Rectangle((float(t_start), float(y_bottom)), ww, band_height, transform=ax.transData)
                )
                task_colors.append(color)

    # Draw patches directly; avoids PatchCollection transform issues on some envs.
    for p in worker_patches:
        p.set_facecolor(WORKER_COLOR)
        p.set_alpha(WORKER_ALPHA)
        p.set_edgecolor("none")
        ax.add_patch(p)

    for p, c in zip(task_patches, task_colors):
        p.set_facecolor(c)
        p.set_alpha(1.0)
        p.set_edgecolor("none")
        ax.add_patch(p)

    legend_handles = [
        Patch(facecolor=WORKER_COLOR, alpha=WORKER_ALPHA, edgecolor="none", label="Workers"),
        Patch(
            facecolor=PHASE_COLORS["successful-committing-to-worker"],
            edgecolor="none",
            label="Successful: Committing",
        ),
        Patch(
            facecolor=PHASE_COLORS["successful-executing-on-worker"],
            edgecolor="none",
            label="Successful: Executing",
        ),
        Patch(
            facecolor=PHASE_COLORS["successful-retrieving-to-manager"],
            edgecolor="none",
            label="Successful: Retrieving",
        ),
    ]
    if has_recovery_successful:
        legend_handles.append(
            Patch(facecolor=PHASE_COLORS["recovery-successful"], edgecolor="none", label="Recovery: Successful")
        )
    if has_recovery_unsuccessful:
        legend_handles.append(
            Patch(facecolor=PHASE_COLORS["recovery-unsuccessful"], edgecolor="none", label="Recovery: Unsuccessful")
        )
    for key in UNSUCCESSFUL_COLORS:
        if key in seen_unsuccessful_types:
            legend_handles.append(
                Patch(
                    facecolor=UNSUCCESSFUL_COLORS[key],
                    edgecolor="none",
                    label=UNSUCCESSFUL_LABELS.get(key, key.replace("-", " ").title()),
                )
            )
    # Keep legend inside the plotting area (top-left) to avoid layout squeezing.
    legend_ncol = min(3, max(1, len(legend_handles)))
    ax.legend(
        handles=legend_handles,
        loc="upper left",
        bbox_to_anchor=(0.01, 0.99),
        bbox_transform=ax.transAxes,
        ncol=legend_ncol,
        frameon=True,
        facecolor="white",
        framealpha=0.85,
        edgecolor="#d0d0d0",
        fontsize=LEGEND_FONT_SIZE,
        handlelength=1.4,
        columnspacing=0.9,
        handletextpad=0.4,
        borderaxespad=0.0,
    )

    ax.set_xlabel("Time (s)", fontsize=LABEL_SIZE, labelpad=X_LABEL_PAD)
    ax.set_ylabel("Worker", fontsize=LABEL_SIZE, labelpad=Y_LABEL_PAD)
    ax.tick_params(axis="both", labelsize=TICK_LABEL_SIZE)

    # X-axis: match vine_report (0, 8007.22, 16014.45, 24021.67, 32028.89)
    x_tick_values = compute_linear_tick_values(x_domain, num_ticks=5, round_digits=2)
    ax.set_xticks(x_tick_values)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.2f} s"))

    # Y-axis: sample ticks from band domain and show worker id only.
    if len(y_domain) <= 5:
        ax.set_yticks(range(len(y_domain)))
        ax.set_yticklabels([band.split("-")[0] for band in y_domain], fontsize=TICK_LABEL_SIZE)
    else:
        num_ticks = 5
        step = (len(y_domain) - 1) / (num_ticks - 1)
        indices = [round(i * step) for i in range(num_ticks)]
        indices = sorted(set(indices))
        ax.set_yticks(indices)
        ax.set_yticklabels([y_domain[i].split("-")[0] for i in indices], fontsize=TICK_LABEL_SIZE)

    ax.set_title("Task Execution Details", fontsize=TITLE_SIZE, pad=TITLE_PAD)

    ensure_dir(png_files_dir, replace=False)
    png_path = os.path.join(png_files_dir, "task-execution-details.png")
    fig.savefig(png_path, dpi=dpi)
    plt.close(fig)
    return png_path
