"""Lightweight helpers shared by parser and reporting code."""

import math
import os
import shutil

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)


TASK_STATUS_NAMES = {
    0: "successful",
    1: "unsuccessful-input-missing",
    2: "unsuccessful-output-missing",
    4: "unsuccessful-stdout-missing",
    1 << 3: "unsuccessful-signal",
    2 << 3: "unsuccessful-resource-exhaustion",
    3 << 3: "unsuccessful-max-end-time",
    4 << 3: "unsuccessful-unknown",
    5 << 3: "unsuccessful-forsaken",
    6 << 3: "unsuccessful-max-retries",
    7 << 3: "unsuccessful-max-wall-time",
    8 << 3: "unsuccessful-monitor-error",
    9 << 3: "unsuccessful-output-transfer-error",
    10 << 3: "unsuccessful-location-missing",
    11 << 3: "unsuccessful-cancelled",
    12 << 3: "unsuccessful-library-exit",
    13 << 3: "unsuccessful-sandbox-exhaustion",
    14 << 3: "unsuccessful-missing-library",
    15 << 3: "unsuccessful-worker-disconnected",
    42 << 3: "undispatched",
    43 << 3: "failed-to-dispatch",
}


def floor_decimal(x, decimal_places):
    factor = 10 ** decimal_places
    return math.floor(x * factor) / factor


def get_size_unit_and_scale(max_file_size_mb) -> tuple[str, float]:
    if max_file_size_mb >= 1024 * 1024:
        return "TB", 1 / (1024 * 1024)
    if max_file_size_mb >= 1024:
        return "GB", 1 / 1024
    if max_file_size_mb >= 1:
        return "MB", 1
    if max_file_size_mb >= 1 / 1024:
        return "KB", 1024
    return "Bytes", 1024 * 1024


def ensure_dir(path, replace=False):
    if os.path.exists(path):
        if replace:
            shutil.rmtree(path)
        else:
            return
    os.makedirs(path)


def count_elements_after(item, values):
    try:
        index = values.index(item)
        return len(values) - index - 1
    except ValueError:
        return -1


def create_progress_bar():
    return Progress(
        SpinnerColumn(),
        "[progress.description]{task.description}",
        BarColumn(),
        MofNCompleteColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        refresh_per_second=2,
    )
