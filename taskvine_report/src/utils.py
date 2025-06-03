from pathlib import Path
import math
import bisect
import random

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