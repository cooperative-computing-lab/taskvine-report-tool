#!/usr/bin/env python3
"""
vine_export command - Export TaskVine report to static PNG and PDF

Reads CSV output from vine_parse, generates static plots (PNG) for each section,
saves to png-files/, and combines into a single PDF in pdf-files/.
"""

import argparse
import base64
import csv
import json
import os
import sys
import fnmatch
import traceback as tb
from datetime import datetime
from html import escape
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from src.utils import check_pip_updates, ensure_dir, create_progress_bar
from src.vine_export.config import (
    CANVAS_HEIGHT_INCHES,
    CANVAS_WIDTH_INCHES,
    DPI_DEFAULT,
    MAX_TASKS_PER_TYPE,
    HTML_DEFAULT_CONTENT_WIDTH_PERCENT,
    HTML_MIN_CONTENT_WIDTH_PERCENT,
    HTML_MAX_CONTENT_WIDTH_PERCENT,
    HTML_EFFECTIVE_MIN_CONTENT_WIDTH_PERCENT,
    HTML_MAX_CONTENT_WIDTH_VIEWPORT_PERCENT,
    HTML_WRAP_PADDING,
    HTML_H1_FONT_SIZE_PX,
    HTML_SUBTEXT_FONT_SIZE_PX,
    HTML_SECTION_TITLE_FONT_SIZE_PX,
    HTML_CARD_BORDER_RADIUS_PX,
    HTML_CARD_BORDER_COLOR,
    HTML_CARD_BG_COLOR,
    HTML_KNOB_PADDING,
    HTML_KNOB_MARGIN_BOTTOM_PX,
    HTML_KNOB_ROW_GAP_PX,
    HTML_SLIDER_MAX_WIDTH_PX,
    HTML_SECTION_CARD_PADDING,
    HTML_SECTION_CARD_MARGIN_Y_PX,
    HTML_IMAGE_BORDER_RADIUS_PX,
    HTML_IMAGE_BORDER_COLOR,
    HTML_TOC_COLUMNS,
    HTML_OVERVIEW_GRID_MIN_COL_WIDTH_PX,
    HTML_OVERVIEW_CARD_PADDING,
    HTML_OVERVIEW_LABEL_FONT_SIZE_PX,
    HTML_OVERVIEW_VALUE_FONT_SIZE_PX,
    HTML_OVERVIEW_LABEL_COLOR,
)
from src import __version__


class ExportHelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter):
    """Keep defaults + preserve line breaks in help text."""
    pass


def remove_duplicates_preserve_order(seq):
    seen = set()
    result = []
    for item in seq:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def normalize_template_pattern(raw_pattern):
    """Normalize template input to rightmost name-only pattern."""
    cleaned = str(raw_pattern).strip().strip("'\"")
    normalized_path = os.path.normpath(cleaned.rstrip("/"))
    return os.path.basename(normalized_path)


def find_matching_directories(root_dir, patterns):
    """Find directories matching given patterns under root_dir."""
    try:
        all_dirs = [
            d for d in os.listdir(root_dir)
            if os.path.isdir(os.path.join(root_dir, d))
        ]
        matched_dirs = []
        for pattern in patterns:
            # Always match by rightmost name under --logs-dir only.
            pattern = normalize_template_pattern(pattern)
            pattern_matches = [d for d in all_dirs if fnmatch.fnmatch(d, pattern)]
            if pattern_matches:
                matched_dirs.extend(pattern_matches)
            else:
                print(f"⚠️  Pattern '{pattern}' matched no directories")
        if not matched_dirs:
            print(f"❌ No directories matched any of the provided patterns in {root_dir}")
            sys.exit(1)
        return matched_dirs
    except Exception as e:
        print(f"❌ Error scanning directory {root_dir}: {e}")
        sys.exit(1)


def has_csv_files(path):
    """Check if directory contains vine_parse output (csv-files subdir)."""
    csv_dir = os.path.join(path, "csv-files")
    return os.path.isdir(csv_dir)


def get_export_dirs(runtime_template):
    """Return paths for png-files and pdf-files (same level as svg-files)."""
    base = Path(runtime_template)
    return {
        "png_files": base / "png-files",
        "pdf_files": base / "pdf-files",
        "html_files": base / "html-files",
        "csv_files": base / "csv-files",
    }


# --- Plot section IDs (aligned with vine_report modules) ---
EXPORT_SECTIONS = [
    "task-execution-details",
    "task-concurrency",
    "task-response-time",
    "task-execution-time",
    "task-retrieval-time",
    "worker-concurrency",
    "worker-storage-consumption",
    "worker-incoming-transfers",
    "worker-outgoing-transfers",
    "worker-executing-tasks",
    "worker-lifetime",
    "file-sizes",
    "file-concurrent-replicas",
    "file-retention-time",
    "file-transferred-size",
    "file-created-size",
]


def generate_plot_png(section_id, csv_files_dir, png_files_dir, **kwargs):
    """
    Generate a single plot PNG for the given section.

    Reads CSV from csv_files_dir, produces PNG in png_files_dir.
    """
    from src.vine_export.plot_sections import (
        plot_task_concurrency,
        plot_task_response_time,
        plot_task_execution_time,
        plot_task_retrieval_time,
        plot_worker_concurrency,
        plot_worker_storage_consumption,
        plot_worker_incoming_transfers,
        plot_worker_outgoing_transfers,
        plot_worker_executing_tasks,
        plot_worker_lifetime,
        plot_file_sizes,
        plot_file_concurrent_replicas,
        plot_file_retention_time,
        plot_file_transferred_size,
        plot_file_created_size,
    )
    from src.vine_export.plot_task_execution_details import plot_task_execution_details

    dpi = kwargs.get("dpi", DPI_DEFAULT)
    max_tasks = kwargs.get("max_tasks", MAX_TASKS_PER_TYPE)
    width = kwargs.get("width")
    height = kwargs.get("height")

    plotters = {
        "task-concurrency": lambda: plot_task_concurrency(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "task-response-time": lambda: plot_task_response_time(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "task-execution-time": lambda: plot_task_execution_time(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "task-retrieval-time": lambda: plot_task_retrieval_time(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "worker-concurrency": lambda: plot_worker_concurrency(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "worker-storage-consumption": lambda: plot_worker_storage_consumption(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "worker-incoming-transfers": lambda: plot_worker_incoming_transfers(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "worker-outgoing-transfers": lambda: plot_worker_outgoing_transfers(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "worker-executing-tasks": lambda: plot_worker_executing_tasks(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "worker-lifetime": lambda: plot_worker_lifetime(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "file-sizes": lambda: plot_file_sizes(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "file-concurrent-replicas": lambda: plot_file_concurrent_replicas(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "file-retention-time": lambda: plot_file_retention_time(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "file-transferred-size": lambda: plot_file_transferred_size(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "file-created-size": lambda: plot_file_created_size(csv_files_dir, png_files_dir, dpi=dpi, width=width, height=height),
        "task-execution-details": lambda: plot_task_execution_details(
            csv_files_dir,
            png_files_dir,
            dpi=dpi,
            max_tasks=max_tasks,
            width=width,
            height=height,
        ),
    }
    if section_id in plotters:
        return plotters[section_id]()
    return None


def combine_pngs_to_pdf(png_paths, pdf_path, **kwargs):
    """
    Combine multiple PNG files into a single PDF.

    TODO: Implement using reportlab, img2pdf, or similar.
    """
    # Placeholder - actual implementation will merge PNGs into one PDF
    pass


def _section_title(section_id):
    return section_id.replace("-", " ").title()


def _load_export_favicon_data_uri():
    """Load vine_report favicon and return a data URI."""
    favicon_path = Path(__file__).resolve().parents[1] / "vine_report" / "static" / "favicon.ico"
    if not favicon_path.exists():
        return None
    try:
        raw = favicon_path.read_bytes()
    except OSError:
        return None
    b64 = base64.b64encode(raw).decode("ascii")
    return f"data:image/x-icon;base64,{b64}"


def _safe_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _read_metadata_dict(csv_files_dir):
    metadata_path = Path(csv_files_dir) / "metadata.csv"
    if not metadata_path.exists():
        return {}
    data = {}
    with open(metadata_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            k = row.get("key")
            v = row.get("value")
            if not k:
                continue
            try:
                data[k] = json.loads(v) if isinstance(v, str) else v
            except json.JSONDecodeError:
                data[k] = v
    return data


def _column_mean(csv_path, column_name):
    path = Path(csv_path)
    if not path.exists():
        return None
    vals = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            num = _safe_float(row.get(column_name))
            if num is not None:
                vals.append(num)
    if not vals:
        return None
    return sum(vals) / len(vals)


def _column_mean_any(csv_path, column_names):
    path = Path(csv_path)
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return None
        target = next((c for c in column_names if c in reader.fieldnames), None)
        if target is None:
            return None
        vals = []
        for row in reader:
            num = _safe_float(row.get(target))
            if num is not None:
                vals.append(num)
    if not vals:
        return None
    return sum(vals) / len(vals)


def _column_max(csv_path, column_name):
    path = Path(csv_path)
    if not path.exists():
        return None
    max_v = None
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            num = _safe_float(row.get(column_name))
            if num is None:
                continue
            if max_v is None or num > max_v:
                max_v = num
    return max_v


def _sum_series_max(csv_path):
    path = Path(csv_path)
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return None
        skip_cols = {"time", "workflow_completion_percentage"}
        candidates = [c for c in reader.fieldnames if c not in skip_cols]
        if not candidates:
            return None
        max_by_col = {c: 0.0 for c in candidates}
        for row in reader:
            for c in candidates:
                num = _safe_float(row.get(c))
                if num is not None and num > max_by_col[c]:
                    max_by_col[c] = num
    return sum(max_by_col.values())


def _max_total_across_rows(csv_path):
    path = Path(csv_path)
    if not path.exists():
        return None
    max_total = None
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return None
        skip_cols = {"time", "workflow_completion_percentage"}
        candidates = [c for c in reader.fieldnames if c not in skip_cols]
        for row in reader:
            total = 0.0
            has_val = False
            for c in candidates:
                num = _safe_float(row.get(c))
                if num is not None:
                    total += num
                    has_val = True
            if has_val and (max_total is None or total > max_total):
                max_total = total
    return max_total


def _format_int(v):
    if v is None:
        return "--"
    try:
        return f"{int(round(float(v))):,}"
    except (TypeError, ValueError):
        return "--"


def _format_float(v, digits=2):
    if v is None:
        return "--"
    return f"{float(v):,.{digits}f}"


def _format_duration(seconds):
    if seconds is None:
        return "--"
    seconds = max(0, float(seconds))
    if seconds < 60:
        return f"{seconds:.1f}s"
    mins = int(seconds // 60)
    secs = seconds % 60
    if mins < 60:
        return f"{mins}m {secs:.0f}s"
    hours = mins // 60
    mins = mins % 60
    return f"{hours}h {mins}m"


def _format_size_mb(mb):
    if mb is None:
        return "--"
    mb = float(mb)
    if mb >= 1024:
        return f"{mb / 1024:.2f} GB"
    return f"{mb:.2f} MB"


def _build_overview_metrics(csv_files_dir):
    csv_dir = Path(csv_files_dir)
    meta = _read_metadata_dict(csv_dir)

    total_submitted = meta.get("total_all_tasks", meta.get("total_tasks"))
    successful = meta.get("successful_tasks")
    failed = meta.get("failed_tasks", meta.get("unsuccessful_tasks"))
    recovery = meta.get("recovery_tasks")
    undispatched = meta.get("undispatched_tasks")
    total_workers = meta.get("total_workers")

    manager_start = _safe_float(meta.get("manager_start_time"))
    first_task_time = _column_max(csv_dir / "time_domain.csv", "MIN_TIME")
    task_window = None
    min_t = _column_max(csv_dir / "time_domain.csv", "MIN_TIME")
    max_t = _column_max(csv_dir / "time_domain.csv", "MAX_TIME")
    if min_t is not None and max_t is not None:
        task_window = max_t - min_t
    wait_before_first_task = None
    if manager_start is not None and first_task_time is not None:
        wait_before_first_task = max(0.0, first_task_time - manager_start)

    avg_task_exec = _column_mean(csv_dir / "task_execution_time.csv", "Execution Time")
    avg_worker_lifetime = _column_mean(csv_dir / "worker_lifetime.csv", "LifeTime (s)")
    total_created_mb = _column_max(csv_dir / "file_created_size.csv", "cumulative_size_mb")
    total_transferred_mb = _column_max(csv_dir / "file_transferred_size.csv", "cumulative_size_mb")
    avg_replica = _column_mean(csv_dir / "file_concurrent_replicas.csv", "max_simul_replicas")
    avg_file_kb = _column_mean_any(
        csv_dir / "file_sizes.csv",
        ["file_size_kb", "size_kb", "File Size (KB)", "File Size"],
    )
    if avg_file_kb is None:
        # Fallback: estimate from total created size / total files.
        total_files = _safe_float(meta.get("total_files"))
        if total_files and total_files > 0 and total_created_mb is not None:
            avg_file_kb = (float(total_created_mb) * 1024.0) / total_files
    incoming_count = _sum_series_max(csv_dir / "worker_incoming_transfers.csv")
    outgoing_count = _sum_series_max(csv_dir / "worker_outgoing_transfers.csv")
    peak_total_storage = _max_total_across_rows(csv_dir / "worker_storage_consumption.csv")

    metrics = [
        ("Tasks Submitted", _format_int(total_submitted), "All submitted tasks (including library tasks)"),
        ("Tasks Succeeded", _format_int(successful), "Completed successfully"),
        ("Tasks Failed", _format_int(failed), "Dispatched but unsuccessful"),
        ("Undispatched Tasks", _format_int(undispatched), "Never dispatched to workers"),
        ("Recovery Tasks", _format_int(recovery), "Tasks marked as recovery"),
        ("Workers Connected", _format_int(total_workers), "Unique worker entries observed"),
        ("Manager -> First Task", _format_duration(wait_before_first_task), "Startup wait before first dispatch"),
        ("First -> Last Task Window", _format_duration(task_window), "Task activity window"),
        ("Avg Task Execution", _format_duration(avg_task_exec), "Mean task execution time"),
        ("Avg Worker Lifetime", _format_duration(avg_worker_lifetime), "Mean worker connection lifetime"),
        ("Total Created Data", _format_size_mb(total_created_mb), "Max cumulative file-created size"),
        ("Total Transferred Data", _format_size_mb(total_transferred_mb), "Max cumulative transferred size"),
        ("Avg Concurrent Replicas", _format_float(avg_replica, 2), "Mean max replicas per file"),
        ("Avg File Size", f"{_format_float((avg_file_kb or 0) / 1024, 2)} MB" if avg_file_kb is not None else "--", "Mean file size"),
        ("Incoming Transfers", _format_int(incoming_count), "Summed per-worker incoming transfer totals"),
        ("Outgoing Transfers", _format_int(outgoing_count), "Summed per-worker outgoing transfer totals"),
        ("Peak Total Worker Storage", _format_size_mb(peak_total_storage), "Peak sum across all workers"),
    ]
    return metrics


def build_self_contained_html_report(items, html_path, template_name, csv_files_dir):
    """
    Build a single self-contained HTML report.

    items: list of dicts with keys {"section_id", "png_path"}.
    """
    ensure_dir(str(Path(html_path).parent), replace=False)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    repo_url = "https://github.com/cooperative-computing-lab/taskvine-report-tool"
    favicon_data_uri = _load_export_favicon_data_uri()
    favicon_link = (
        f'<link rel="icon" href="{favicon_data_uri}" type="image/x-icon"/>'
        if favicon_data_uri
        else ""
    )
    overview_metrics = _build_overview_metrics(csv_files_dir)
    overview_cards = "".join(
        f"""
        <div class="metric">
          <div class="metric-label">{escape(label)}</div>
          <div class="metric-value">{escape(value)}</div>
          <div class="metric-note">{escape(note)}</div>
        </div>
        """
        for label, value, note in overview_metrics
    )

    toc_rows = []
    section_blocks = []
    for idx, item in enumerate(items, start=1):
        section_id = item["section_id"]
        png_path = item["png_path"]
        anchor = f"sec-{idx}"
        title = _section_title(section_id)
        try:
            with open(png_path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode("ascii")
        except OSError:
            # Skip broken image paths to keep report generation robust.
            continue

        toc_rows.append(f'<li><a href="#{anchor}">{escape(title)}</a></li>')
        section_blocks.append(
            f"""
            <section class="card" id="{anchor}">
              <h2>{escape(title)}</h2>
              <img src="data:image/png;base64,{b64}" alt="{escape(title)}"/>
            </section>
            """
        )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  {favicon_link}
  <title>TaskVine Report - {escape(template_name)}</title>
  <style>
    :root {{
      --content-width-percent: {HTML_DEFAULT_CONTENT_WIDTH_PERCENT};
    }}
    body {{
      margin: 0;
      background: #f6f8fb;
      color: #1f2937;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
      line-height: 1.45;
    }}
    .wrap {{
      width: min(calc(var(--content-width-percent) * 1vw), {HTML_MAX_CONTENT_WIDTH_VIEWPORT_PERCENT}vw);
      margin: 0 auto;
      padding: {HTML_WRAP_PADDING};
    }}
    .layout-knob {{
      background: {HTML_CARD_BG_COLOR};
      border: 1px solid {HTML_CARD_BORDER_COLOR};
      border-radius: {HTML_CARD_BORDER_RADIUS_PX}px;
      padding: {HTML_KNOB_PADDING};
      margin-bottom: {HTML_KNOB_MARGIN_BOTTOM_PX}px;
      font-size: {HTML_SUBTEXT_FONT_SIZE_PX}px;
    }}
    .layout-knob .row {{
      display: flex;
      align-items: center;
      justify-content: center;
      gap: {HTML_KNOB_ROW_GAP_PX}px;
      flex-wrap: nowrap;
    }}
    .layout-knob .row strong {{
      font-size: {HTML_SECTION_TITLE_FONT_SIZE_PX}px;
      line-height: 1.2;
    }}
    .layout-knob input[type="range"] {{
      width: min({HTML_SLIDER_MAX_WIDTH_PX}px, 80vw);
    }}
    .layout-knob .value {{
      min-width: 56px;
      color: #1d4ed8;
      font-weight: 600;
      white-space: nowrap;
      display: inline-block;
    }}
    h1 {{
      margin: 0 0 4px;
      font-size: {HTML_H1_FONT_SIZE_PX}px;
      font-weight: 700;
    }}
    .sub {{
      color: #667085;
      font-size: {HTML_SUBTEXT_FONT_SIZE_PX}px;
      margin-bottom: 16px;
    }}
    .repo {{
      margin: 0 0 16px;
      font-size: {HTML_SUBTEXT_FONT_SIZE_PX}px;
    }}
    .repo a {{
      color: #1d4ed8;
      text-decoration: none;
    }}
    .repo a:hover {{
      text-decoration: underline;
    }}
    .toc {{
      background: {HTML_CARD_BG_COLOR};
      border: 1px solid {HTML_CARD_BORDER_COLOR};
      border-radius: {HTML_CARD_BORDER_RADIUS_PX}px;
      padding: 12px 14px;
      margin-bottom: 16px;
    }}
    .overview {{
      margin-bottom: 16px;
    }}
    .overview h2 {{
      margin: 0 0 10px;
      font-size: {HTML_SECTION_TITLE_FONT_SIZE_PX}px;
    }}
    .overview-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax({HTML_OVERVIEW_GRID_MIN_COL_WIDTH_PX}px, 1fr));
      gap: 10px;
    }}
    .metric {{
      border: 1px solid {HTML_CARD_BORDER_COLOR};
      border-radius: 8px;
      padding: {HTML_OVERVIEW_CARD_PADDING};
      background: #fafcff;
    }}
    .metric-label {{
      font-size: {HTML_OVERVIEW_LABEL_FONT_SIZE_PX}px;
      color: {HTML_OVERVIEW_LABEL_COLOR};
      margin-bottom: 2px;
    }}
    .metric-value {{
      font-size: {HTML_OVERVIEW_VALUE_FONT_SIZE_PX}px;
      font-weight: 700;
      color: #0f172a;
      margin-bottom: 2px;
      white-space: nowrap;
    }}
    .metric-note {{
      font-size: 11px;
      color: #98a2b3;
    }}
    .toc ul {{
      margin: 8px 0 0;
      padding-left: 18px;
      columns: {HTML_TOC_COLUMNS};
    }}
    .toc a {{
      color: #1d4ed8;
      text-decoration: none;
    }}
    .toc a:hover {{
      text-decoration: underline;
    }}
    .toc strong {{
      font-size: {HTML_SECTION_TITLE_FONT_SIZE_PX}px;
      line-height: 1.2;
    }}
    .card {{
      background: {HTML_CARD_BG_COLOR};
      border: 1px solid {HTML_CARD_BORDER_COLOR};
      border-radius: {HTML_CARD_BORDER_RADIUS_PX}px;
      padding: {HTML_SECTION_CARD_PADDING};
      margin: {HTML_SECTION_CARD_MARGIN_Y_PX}px 0;
    }}
    .card h2 {{
      margin: 0 0 4px;
      font-size: {HTML_SECTION_TITLE_FONT_SIZE_PX}px;
    }}
    img {{
      display: block;
      width: 100%;
      height: auto;
      border: 1px solid {HTML_IMAGE_BORDER_COLOR};
      border-radius: {HTML_IMAGE_BORDER_RADIUS_PX}px;
      background: #fff;
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <div class="layout-knob">
      <div class="row">
        <strong>Content Width</strong>
        <input id="width-slider" type="range" min="{HTML_MIN_CONTENT_WIDTH_PERCENT}" max="{HTML_MAX_CONTENT_WIDTH_PERCENT}" step="1" value="{HTML_DEFAULT_CONTENT_WIDTH_PERCENT}"/>
        <span class="value" id="width-value">{HTML_DEFAULT_CONTENT_WIDTH_PERCENT}%</span>
      </div>
    </div>
    <h1>TaskVine Export Report</h1>
    <div class="sub">Template: {escape(template_name)}</div>
    <div class="sub">Generated: {escape(generated_at)}</div>
    <div class="repo">GitHub: <a href="{repo_url}" target="_blank" rel="noopener noreferrer">{repo_url}</a></div>
    <section class="card overview">
      <h2>Overview</h2>
      <div class="overview-grid">
        {overview_cards}
      </div>
    </section>
    <nav class="toc">
      <strong>Sections</strong>
      <ul>
        {''.join(toc_rows)}
      </ul>
    </nav>
    {''.join(section_blocks)}
  </main>
  <script>
    (function () {{
      const slider = document.getElementById('width-slider');
      const valueText = document.getElementById('width-value');
      if (!slider || !valueText) return;
      const effectiveMin = {HTML_EFFECTIVE_MIN_CONTENT_WIDTH_PERCENT};
      let pendingValue = Math.max(effectiveMin, Number(slider.value));
      slider.value = String(pendingValue);
      const apply = (v) => {{
        const clamped = Math.max(effectiveMin, Number(v));
        document.documentElement.style.setProperty('--content-width-percent', String(clamped));
        valueText.textContent = `${{clamped}}%`;
      }};
      apply(pendingValue);
      // During dragging, only update label; apply layout change on release.
      slider.addEventListener('input', (e) => {{
        pendingValue = Math.max(effectiveMin, Number(e.target.value));
        e.target.value = String(pendingValue);
        valueText.textContent = `${{pendingValue}}%`;
      }});
      const commit = () => apply(pendingValue);
      slider.addEventListener('change', commit);     // fallback
      slider.addEventListener('mouseup', commit);    // desktop mouse
      slider.addEventListener('touchend', commit);   // mobile touch
      slider.addEventListener('pointerup', commit);  // pointer devices
    }})();
  </script>
</body>
</html>
"""
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    return html_path


def export_single_template(template_path, args, sections_to_export):
    """Export one runtime template: PNGs + PDF."""
    dirs = get_export_dirs(template_path)
    ensure_dir(str(dirs["pdf_files"]), replace=False)

    csv_files_dir = dirs["csv_files"]
    # Use --png-dir if specified, else default to {template}/png-files/
    if getattr(args, "png_dir", None):
        png_base = Path(args.png_dir).resolve()
        png_files_dir = png_base / Path(template_path).name
        html_files_dir = png_base / Path(template_path).name / "html-files"
    else:
        png_files_dir = dirs["png_files"]
        html_files_dir = dirs["html_files"]
    ensure_dir(str(png_files_dir), replace=False)
    ensure_dir(str(html_files_dir), replace=False)
    pdf_files_dir = dirs["pdf_files"]

    if not csv_files_dir.exists():
        print(f"  ⚠️  csv-files not found, skipping: {template_path}")
        return False

    png_items = []
    with create_progress_bar() as progress:
        section_task = progress.add_task(
            f"[green]Exporting sections ({Path(template_path).name})",
            total=len(sections_to_export),
        )
        for section_id in sections_to_export:
            try:
                section_kwargs = {
                    "dpi": getattr(args, "dpi", DPI_DEFAULT),
                    "max_tasks": getattr(args, "max_tasks", MAX_TASKS_PER_TYPE),
                    "width": getattr(args, "width", CANVAS_WIDTH_INCHES),
                    "height": getattr(args, "height", CANVAS_HEIGHT_INCHES),
                }
                png_path = generate_plot_png(
                    section_id,
                    str(csv_files_dir),
                    str(png_files_dir),
                    **section_kwargs,
                )
                if png_path and os.path.exists(png_path):
                    png_items.append({"section_id": section_id, "png_path": png_path})
            except Exception as e:
                print(f"  ⚠️  Failed to generate {section_id}: {e}")
            finally:
                progress.advance(section_task)

    # PDF generation disabled for now (only Task Execution Details supported)
    # if png_paths:
    #     pdf_name = Path(template_path).name + ".pdf"
    #     pdf_path = pdf_files_dir / pdf_name
    #     try:
    #         combine_pngs_to_pdf(...)
    #     except Exception as e:
    #         ...

    if png_items:
        html_path = html_files_dir / f"{Path(template_path).name}.html"
        build_self_contained_html_report(
            png_items,
            str(html_path),
            Path(template_path).name,
            str(csv_files_dir),
        )
        print(f"  ✅ PNG saved to directory: {png_files_dir}")
        print(f"  ✅ HTML saved to: {html_path}")
    else:
        print("  ⚠️  No PNG generated for this template")

    return True


def main():
    sections_with_all = sorted(EXPORT_SECTIONS + ["all"])
    sections_help_lines = "\n".join([f"  - {s}" for s in sections_with_all])

    parser = argparse.ArgumentParser(
        prog="vine_export",
        description="Export TaskVine report to static PNG and PDF from vine_parse CSV output",
        formatter_class=ExportHelpFormatter,
    )

    parser.add_argument(
        "--logs-dir",
        default=os.getcwd(),
        help="Base directory containing parsed log folders (default: current directory)",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--templates",
        type=str,
        nargs="+",
        help=(
            "List of log directory names/patterns (matched under --logs-dir; must have csv-files). "
            "Each item is normalized to its rightmost name. "
            "Use quotes to avoid shell pre-expansion, e.g. --templates \"exp*\" \"test*\"."
        ),
    )
    group.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        help="Recursively find all directories with csv-files",
    )

    parser.add_argument(
        "--png-dir",
        type=str,
        default=None,
        help="Output directory for PNG files (default: {template}/png-files/ under logs-dir)",
    )

    parser.add_argument(
        "--dpi",
        type=int,
        default=DPI_DEFAULT,
        help="DPI for PNG/PDF output",
    )

    parser.add_argument(
        "--max-tasks",
        type=int,
        default=MAX_TASKS_PER_TYPE,
        help="Max tasks per type to plot (reduce if OOM)",
    )

    parser.add_argument(
        "--width",
        type=float,
        default=CANVAS_WIDTH_INCHES,
        help="Canvas width in inches",
    )
    parser.add_argument(
        "--height",
        type=float,
        default=CANVAS_HEIGHT_INCHES,
        help="Canvas height in inches",
    )

    parser.add_argument(
        "--sections",
        type=str,
        nargs="+",
        default=["all"],
        metavar="SECTION",
        help=(
            "Only export these section IDs (default: all).\n"
            "Use 'all' to export all sections.\n"
            "Available sections:\n"
            f"{sections_help_lines}"
        ),
    )

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    args = parser.parse_args()
    # Resolve selected sections
    if args.sections == ["all"]:
        selected_sections = EXPORT_SECTIONS[:]
    else:
        normalized = [s.strip() for s in args.sections]
        if "all" in normalized:
            selected_sections = EXPORT_SECTIONS[:]
        else:
            invalid = [s for s in normalized if s not in EXPORT_SECTIONS]
            if invalid:
                print(f"❌ Unknown section(s): {', '.join(invalid)}")
                print(f"   Valid sections: {', '.join(sorted(EXPORT_SECTIONS))}")
                sys.exit(1)
            # Keep export arrangement consistent with front-end section order.
            selected_sections = [s for s in EXPORT_SECTIONS if s in set(normalized)]

    check_pip_updates()

    root_dir = os.path.abspath(args.logs_dir)

    if args.recursive:
        full_paths = [
            str(p.parent)
            for p in Path(root_dir).rglob("csv-files")
            if p.is_dir()
        ]
        full_paths = remove_duplicates_preserve_order(full_paths)
    else:
        matched_dirs = find_matching_directories(root_dir, args.templates)
        deduped = remove_duplicates_preserve_order(matched_dirs)
        full_paths = [os.path.join(root_dir, name) for name in deduped]

    full_paths = [str(Path(p).resolve()) for p in full_paths]
    full_paths = remove_duplicates_preserve_order(full_paths)

    # Filter: only dirs that have csv-files (vine_parse output)
    valid_paths = [p for p in full_paths if has_csv_files(p)]
    missing_csv = [p for p in full_paths if not has_csv_files(p)]

    if missing_csv:
        print("⚠️  The following directories do not contain csv-files (run vine_parse first):")
        for m in missing_csv:
            print(f"  - {m}")

    if not valid_paths:
        print("❌ No valid directories with csv-files found to export")
        sys.exit(1)

    print(f"\n✅ Exporting {len(valid_paths)} log director{'y' if len(valid_paths) == 1 else 'ies'}:")
    for path in valid_paths:
        print(f"  - {path}")

    success = 0
    failed = 0

    for template in valid_paths:
        print(f"\n=== Exporting: {template}")
        try:
            export_single_template(template, args, selected_sections)
            success += 1
            print(f"✅ Successfully exported: {template}")
        except Exception as e:
            failed += 1
            print(f"❌ Error exporting {template}")
            print(tb.format_exc())

    if success > 0:
        print(f"\n🎉 {success} director{'y' if success == 1 else 'ies'} exported successfully!")
    if failed > 0:
        print(f"❌ {failed} director{'y' if failed == 1 else 'ies'} failed to export")


if __name__ == "__main__":
    main()
