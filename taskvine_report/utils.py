from pathlib import Path
import math
import bisect
import random
import os
import hashlib
import math
import shutil
import random
import bisect
import numpy as np
import functools
import json
import pandas as pd
from flask import current_app

def floor_decimal(x, decimal_places):
    factor = 10 ** decimal_places
    return math.floor(x * factor) / factor

def get_size_unit_and_scale(max_file_size_mb) -> tuple[str, float]:
    if max_file_size_mb >= 1024 * 1024:
        return 'TB', 1 / (1024 * 1024)
    elif max_file_size_mb >= 1024:
        return 'GB', 1 / 1024
    elif max_file_size_mb >= 1:
        return 'MB', 1
    elif max_file_size_mb >= 1 / 1024:
        return 'KB', 1024
    else:
        return 'Bytes', 1024 * 1024

def file_list_formatter(file_list):
    return ', '.join([f for f in file_list if not f.startswith('file-meta-') and not f.startswith('file-buffer-')])

def compute_linear_tick_values(domain, num_ticks=5, round_digits=2):
    start, end = domain
    if num_ticks < 2:
        raise ValueError("num_ticks must be at least 2")
    step = (end - start) / (num_ticks - 1)

    values = [float(start + i * step) for i in range(num_ticks)]

    if round_digits is None:
        return values
    elif round_digits == 0:
        return [int(v) for v in values]
    else:
        return [round(v, round_digits) for v in values]

def compute_discrete_tick_values(domain_list, num_ticks=5):
    if not domain_list:
        return []
    
    if len(domain_list) <= num_ticks * 2:
        return [float(x) for x in domain_list]
    
    domain_list = sorted(set(domain_list))
    n = len(domain_list)

    if n <= num_ticks:
        return [float(x) for x in domain_list]

    tick_indices = [0]
    for i in range(1, num_ticks - 1):
        idx = round(i * (n - 1) / (num_ticks - 1))
        tick_indices.append(idx)
    tick_indices.append(n - 1)

    tick_indices = sorted(set(tick_indices))
    return [float(domain_list[i]) for i in tick_indices]

def d3_time_formatter():
    return '(d) => d3.format(".2f")(d) + " s"'

def d3_int_formatter():
    return '(d) => d3.format(".0f")(d)'

def d3_size_formatter(unit):
    return f'(d) => d3.format(".2f")(d) + " {unit}"'

def d3_percentage_formatter(digits=2):
    return f'(d) => d3.format(".{digits}f")(d) + " %"'

def d3_worker_core_formatter():
    return '(d) => d.split("-")[0]'

def downsample_points(points, target_point_count=10000, y_index=1):
    if not points:
        return []

    tuple_len = len(points[0])
    if tuple_len < 2:
        raise ValueError("Each point must have at least 2 elements (x, y)")
    if tuple_len > 2 and y_index is None:
        raise ValueError("y_index must be specified when tuple length > 2")
    if y_index >= tuple_len:
        raise IndexError(f"y_index {y_index} is out of bounds for tuple of length {tuple_len}")

    if len(points) <= target_point_count:
        return points

    MIN_POINT_COUNT = 500
    if len(points) > MIN_POINT_COUNT and target_point_count < MIN_POINT_COUNT:
        target_point_count = MIN_POINT_COUNT

    valid_points = [(i, p) for i, p in enumerate(points) if p[y_index] is not None]
    if not valid_points:
        return points[:target_point_count]

    y_max_idx = max(valid_points, key=lambda x: x[1][y_index])[0]
    keep_indices = {0, len(points) - 1, y_max_idx}

    remaining = target_point_count - len(keep_indices)
    if remaining <= 0:
        return [points[i] for i in sorted(keep_indices)]

    sorted_indices = sorted(keep_indices)
    points_per_gap = remaining // (len(sorted_indices) - 1)
    extra = remaining % (len(sorted_indices) - 1)

    for i in range(len(sorted_indices) - 1):
        start, end = sorted_indices[i], sorted_indices[i + 1]
        gap = end - start - 1
        if gap <= 0:
            continue
        n = points_per_gap + (1 if extra > 0 else 0)
        if extra > 0:
            extra -= 1
        if n > 0:
            available = list(range(start + 1, end))
            if len(available) <= n:
                sampled = available
            else:
                step = len(available) / n
                sampled = [available[int(i * step)] for i in range(n)]
            keep_indices.update(sampled)

    return [points[i] for i in sorted(keep_indices)]

def downsample_series_points(series_points_dict, y_index=1):
    return {
        series: downsample_points(points, y_index=y_index)
        for series, points in series_points_dict.items()
    }

def get_task_produced_files(files, min_time):
    rows = []
    for file in files.values():
        if not file.transfers or not file.producers:
            continue

        fname = file.filename
        created_time = min((t.time_start_stage_in for t in file.transfers), default=float('inf')) - min_time
        created_time = round(created_time, 2) if created_time != float('inf') else float('inf')
        
        rows.append((0, fname, created_time))
    
    return rows

def build_request_info_string(request):
    method = request.method
    path = request.path
    args = dict(request.args)
    headers = {k: v for k, v in request.headers if k not in ['Cookie', 'Authorization']}
    remote_addr = request.remote_addr

    request_info = {
        'method': method,
        'path': path,
        'args': args,
        'headers': headers,
        'remote_addr': remote_addr
    }

    if path.startswith('/api/'):
        return f"API Request: {method} {path} - {request_info}"
    else:
        return f"HTTP Request: {method} {path}"

def build_response_info_string(response, request, duration=None):
    path = request.path
    status_code = response.status_code

    if path.startswith('/api/'):
        if duration:
            return f"API Response: {status_code} for {path} - completed in {duration:.4f}s"
        else:
            return f"API Response: {status_code} for {path}"
    elif status_code >= 400:
        return f"HTTP Error Response: {status_code} for {path}"
    else:
        return f"HTTP Response: {status_code} for {path}"

def get_file_stat(file_path):
    try:
        stat = os.stat(file_path)
        return {
            'mtime': stat.st_mtime,
            'size': stat.st_size
        }
    except Exception:
        return None
    
def get_files_fingerprint(files):
    if not files:
        return None

    parts = []
    for file in files:
        stat = get_file_stat(file)
        if not stat:
            continue
        parts.append(f"{file}:{stat['mtime']}:{stat['size']}")

    return hashlib.md5(";".join(parts).encode()).hexdigest()

def get_worker_ip_port_from_key(key):
    return ':'.join(key.split(':')[:-1])

def get_worker_time_boundary_points(worker, base_time):
    t_connected = floor_decimal(worker.time_connected[0] - base_time, 2)
    t_disconnected = floor_decimal(worker.time_disconnected[0] - base_time, 2)
    boundary = []
    if t_connected > 0:
        boundary.append((t_connected, 0))
    if t_disconnected > 0:
        boundary.append((t_disconnected, 0))
    return boundary

def prefer_zero_else_max(series):
    if (series == 0).any():
        return 0.0
    return series.max()

def check_and_reload_data():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_app.config["RUNTIME_STATE"].reload_data_if_needed()

            response = func(*args, **kwargs)

            if hasattr(response, 'get_json'):
                try:
                    response_data = response.get_json()
                except Exception:
                    response_data = None
            else:
                response_data = response

            if isinstance(response_data, (dict, list)):
                response_size = len(json.dumps(response_data)) if response_data else 0
            elif hasattr(response, 'get_data'):
                response_size = len(response.get_data())
            else:
                response_size = 0

            route_name = func.__name__
            current_app.config["RUNTIME_STATE"].log_info(f"Route {route_name} response size: {response_size/1024/1024:.2f} MB")

            return response
        return wrapper
    return decorator


# check if all required subfolders exist
def all_subfolders_exists(parent: str, folder_names: list[str]) -> bool:
    parent_path = Path(parent).resolve()
    for folder_name in folder_names:
        target_path = parent_path / folder_name
        if not target_path.is_dir():
            return False
    return True

def floor_decimal(x, decimal_places):
    factor = 10 ** decimal_places
    return math.floor(x * factor) / factor

def get_size_unit_and_scale(max_file_size_mb) -> tuple[str, float]:
    if max_file_size_mb >= 1024 * 1024:
        return 'TB', 1 / (1024 * 1024)
    elif max_file_size_mb >= 1024:
        return 'GB', 1 / 1024
    elif max_file_size_mb >= 1:
        return 'MB', 1
    elif max_file_size_mb >= 1 / 1024:
        return 'KB', 1024
    else:
        return 'Bytes', 1024 * 1024
    
def compress_time_based_critical_points(points, max_points=10000):
    n = len(points)
    if n <= max_points:
        return [tuple(p) for p in points]

    points = [tuple(p) for p in points]
    peak_indices = []
    valley_indices = []

    for i in range(1, n - 1):
        prev_y, curr_y, next_y = points[i - 1][1], points[i][1], points[i + 1][1]
        if curr_y > prev_y and curr_y >= next_y:
            peak_indices.append(i)
        elif curr_y < prev_y and curr_y <= next_y:
            valley_indices.append(i)

    keep_indices = set(peak_indices + valley_indices)

    def add_nearest_opposite(from_list, to_list):
        to_list_sorted = sorted(to_list)
        for idx in from_list:
            pos = bisect.bisect_right(to_list_sorted, idx)
            if pos > 0:
                keep_indices.add(to_list_sorted[pos - 1])
            if pos < len(to_list_sorted):
                keep_indices.add(to_list_sorted[pos])

    add_nearest_opposite(peak_indices, valley_indices)
    add_nearest_opposite(valley_indices, peak_indices)

    keep_indices.update([0, n - 1])

    sorted_indices = sorted(keep_indices)
    meaningful_indices = [sorted_indices[0]]

    for i in range(1, len(sorted_indices) - 1):
        a = points[sorted_indices[i - 1]][1]
        b = points[sorted_indices[i]][1]
        c = points[sorted_indices[i + 1]][1]
        if not (a <= b <= c or a >= b >= c):
            meaningful_indices.append(sorted_indices[i])

    meaningful_indices.append(sorted_indices[-1])
    selected_set = set(meaningful_indices)

    if len(selected_set) < max_points:
        remaining = max_points - len(selected_set)
        all_indices = set(range(n))
        candidates = list(all_indices - selected_set)
        if candidates:
            extra = random.sample(candidates, min(remaining, len(candidates)))
            selected_set.update(extra)

    final_indices = sorted(selected_set)
    return [points[i] for i in final_indices]

def read_csv_to_fd(csv_path):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        raise RuntimeError(f"Failed to read CSV file {csv_path}: {e}")
    
    if df.empty:
        raise ValueError(f"CSV file is empty: {csv_path}")
    
    return df

def extract_points_from_df(df, x_col, *y_cols):
    cols = (x_col,) + y_cols
    missing = [col for col in cols if col not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in DataFrame: {missing}")

    points = df[list(cols)].dropna().itertuples(index=False, name=None)
    return list(points)


def extract_series_points_dict(df, x_col):
    if x_col not in df.columns:
        raise KeyError(f"{x_col} not found in DataFrame")
    
    melted = df.melt(id_vars=x_col, var_name='series', value_name='y').dropna()
    
    return {
        series: group[[x_col, 'y']].values.tolist()
        for series, group in melted.groupby('series')
    }

def scale_storage_series_points(storage_data):
    max_value = 0.0

    for points in storage_data.values():
        if not points:
            continue
        if isinstance(points[0], list) and len(points[0]) >= 2:
            y_values = np.array([p[1] for p in points if p[1] is not None], dtype=np.float64)
        else:
            y_values = np.array([p for p in points if p is not None], dtype=np.float64)

        if y_values.size > 0:
            max_value = max(max_value, np.max(y_values))

    if max_value == 0.0:
        return storage_data, "MB"

    unit, scale = get_size_unit_and_scale(max_value)
    if scale == 1:
        return storage_data, unit

    scaled_data = {}
    for series, points in storage_data.items():
        if not points:
            scaled_data[series] = points
            continue

        if isinstance(points[0], list) and len(points[0]) >= 2:
            arr = np.array(points, dtype=np.float64)
            arr[:, 1] *= scale
            scaled_data[series] = arr.tolist()
        else:
            arr = np.array(points, dtype=np.float64)
            arr *= scale
            scaled_data[series] = arr.tolist()

    return scaled_data, unit

def extract_x_range_from_points(points, x_index=0):
    if not points or not isinstance(points, list):
        return [0, 1]

    try:
        xs = [
            p[x_index]
            for p in points
            if isinstance(p, (list, tuple)) and len(p) > x_index and p[x_index] is not None
        ]
        return [min(xs), max(xs)] if xs else [0, 1]
    except Exception:
        return [0, 1]

def extract_y_range_from_points(points, y_index=1):
    if not points or not isinstance(points, list):
        return [0, 1]

    try:
        ys = [
            p[y_index]
            for p in points
            if isinstance(p, (list, tuple)) and len(p) > y_index and p[y_index] is not None
        ]
        return [min(0.0, min(ys)), max(1.0, max(ys))] if ys else [0, 1]
    except Exception:
        return [0, 1]

def extract_xy_domains_from_series_points(series_points_dict):
    x_min = float('inf')
    x_max = float('-inf')
    y_min = float('inf')
    y_max = float('-inf')

    for points in series_points_dict.values():
        if not points:
            continue
        xs, ys = zip(*points)
        x_min = min(x_min, min(xs))
        x_max = max(x_max, max(xs))
        y_min = min(y_min, min(ys))
        y_max = max(y_max, max(ys))

    if x_min == float('inf') or y_min == float('inf'):
        return [0, 1], [0, 1]

    return [x_min, x_max], [min(0.0, y_min), max(1.0, y_max)]

def extract_size_points_from_df(df, x_col, y_col):
    points = extract_points_from_df(df, x_col, y_col)
    unit, scale = get_size_unit_and_scale(max(p[1] for p in points))
    return [[x, y * scale] for x, y in points], unit

def ensure_dir(path):
    if os.path.exists(path):
        return
    os.makedirs(path)