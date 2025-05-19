import os
import hashlib


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

    values = [start + i * step for i in range(num_ticks)]

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
        return domain_list
    
    domain_list = sorted(set(domain_list))
    n = len(domain_list)

    if n <= num_ticks:
        return domain_list

    tick_indices = [0]
    for i in range(1, num_ticks - 1):
        idx = round(i * (n - 1) / (num_ticks - 1))
        tick_indices.append(idx)
    tick_indices.append(n - 1)

    tick_indices = sorted(set(tick_indices))
    return [domain_list[i] for i in tick_indices]

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

def downsample_points(points, target_point_count=10000):
    if len(points) <= target_point_count:
        return points

    MIN_POINT_COUNT = 100
    if len(points) > MIN_POINT_COUNT and target_point_count < MIN_POINT_COUNT:
        target_point_count = MIN_POINT_COUNT

    y_max_idx = max(range(len(points)), key=lambda i: points[i][1])
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

def downsample_points_array(points_array, target_point_count=10000):
    total_points = sum(len(points) for points in points_array)
    if total_points <= target_point_count:
        return points_array

    downsampled_array = []
    for points in points_array:
        proportional_point_count = int((len(points) / total_points) * target_point_count)
        downsampled = downsample_points(points, proportional_point_count)
        downsampled_array.append(downsampled)

    return downsampled_array

def compute_points_domain(points):
    x_domain = [min(points, key=lambda p: p[0])[0], max(points, key=lambda p: p[0])[0]]
    y_domain = [min(points, key=lambda p: p[1])[1], max(points, key=lambda p: p[1])[1]]

    return x_domain, y_domain

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

def select_best_try_per_task(task_stats):
    from collections import defaultdict

    grouped = defaultdict(list)
    for row in task_stats:
        grouped[row['task_id']].append(row)

    filtered_stats = []

    for task_id, rows in grouped.items():
        candidates = [r for r in rows if r['task_response_time'] is not None]
        if not candidates:
            candidates = rows
        sub = [r for r in candidates if r['task_execution_time'] is not None]
        if sub:
            candidates = sub
        sub = [r for r in candidates if r['task_waiting_retrieval_time'] is not None]
        if sub:
            candidates = sub

        best = max(candidates, key=lambda r: r['task_try_id'])

        filtered_stats.append({k: v for k, v in best.items() if k != 'task_try_id'})

    return filtered_stats
