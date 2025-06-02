import os
import hashlib
import math
import random
import bisect
import functools
import json
from flask import current_app

def floor_decimal(x, decimal_places):
    factor = 10 ** decimal_places
    return math.floor(x * factor) / factor

def get_unit_and_scale_by_max_file_size_mb(max_file_size_mb) -> tuple[str, float]:
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

def downsample_points(points, target_point_count=None):
    if not target_point_count:
        target_point_count=current_app.config["SAMPLING_POINTS"]
    if len(points) <= target_point_count:
        return points

    MIN_POINT_COUNT = 100
    if len(points) > MIN_POINT_COUNT and target_point_count < MIN_POINT_COUNT:
        target_point_count = MIN_POINT_COUNT

    valid_points = [(i, p) for i, p in enumerate(points) if p[1] is not None]
    if not valid_points:
        return points[:target_point_count] if len(points) > target_point_count else points

    y_max_idx = max(valid_points, key=lambda x: x[1][1])[0]
    keep_indices = {0, len(points) - 1, y_max_idx}

    remaining = target_point_count - len(keep_indices)
    if remaining <= 0:
        return [points[0], points[y_max_idx], points[-1]]

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

def compute_points_domain(points):
    if not points:
        return [0, 1], [0, 1]

    try:
        points = [tuple(p) for p in points]
        
        if not points or not all(len(p) >= 2 for p in points):
            return [0, 1], [0, 1]
            
        x_values = [p[0] for p in points]
        y_values = [p[1] for p in points]
        
        x_valid = [x for x in x_values if x is not None]
        y_valid = [y for y in y_values if y is not None]
        
        if x_valid:
            x_domain = [min(x_valid), max(x_valid)]
        else:
            x_domain = [0, 1]
            
        if y_valid:
            y_domain = [min(y_valid), max(y_valid)]
        else:
            y_domain = [0, 1]

        return x_domain, y_domain
        
    except Exception:
        return [0, 1], [0, 1]

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

def compress_time_based_critical_points(points, max_points=None):
    if not max_points:
        max_points=current_app.config["SAMPLING_TASK_BARS"]

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