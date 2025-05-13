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

def downsample_points(points, sampling_points=10000):
    if len(points) <= sampling_points:
        return points

    y_max_idx = max(range(len(points)), key=lambda i: points[i][1])
    keep_indices = {0, len(points) - 1, y_max_idx}

    remaining = sampling_points - len(keep_indices)
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

def downsample_points_array(points_array, sampling_points=10000):
    total_points = sum(len(points) for points in points_array)
    if total_points <= sampling_points:
        return points_array

    downsampled_array = []
    for points in points_array:
        proportion = len(points) / total_points
        allocated = max(3, int(proportion * sampling_points))
        downsampled = downsample_points(points, allocated)
        downsampled_array.append(downsampled)

    return downsampled_array

def compute_points_domain(points):
    x_domain = [min(points, key=lambda p: p[0])[0], max(points, key=lambda p: p[0])[0]]
    y_domain = [min(points, key=lambda p: p[1])[1], max(points, key=lambda p: p[1])[1]]

    return x_domain, y_domain

def compute_task_dependency_metrics(tasks, mode='dependencies'):
    output_file_to_task = {}
    for task in tasks.values():
        for f in task.output_files:
            output_file_to_task[f] = task.task_id

    count = {}
    for task in tasks.values():
        if task.is_library_task:
            continue
        count.setdefault(task.task_id, 0)

    if mode == 'dependencies':
        for task in tasks.values():
            if task.is_library_task:
                continue
            parent_tasks = set()
            for f in task.input_files:
                parent_id = output_file_to_task.get(f)
                if parent_id and parent_id != task.task_id:
                    parent_tasks.add(parent_id)
            count[task.task_id] = len(parent_tasks)

    elif mode == 'dependents':
        for task in tasks.values():
            if task.is_library_task:
                continue
            for f in task.input_files:
                producer_id = output_file_to_task.get(f)
                if producer_id and producer_id != task.task_id and producer_id in count:
                    count[producer_id] += 1

    else:
        raise ValueError(f"Unknown mode: {mode}")

    return [[task_id, count[task_id]] for task_id in sorted(count.keys())]
