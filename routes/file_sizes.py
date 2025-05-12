from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from src.utils import get_unit_and_scale_by_max_file_size_mb
from .utils import d3_size_formatter, compute_tick_values, d3_int_formatter

import pandas as pd
import random
from flask import Blueprint, jsonify, request

file_sizes_bp = Blueprint('file_sizes', __name__, url_prefix='/api')


def downsample_file_sizes(points):
    # downsample while keeping key points
    if len(points) <= SAMPLING_POINTS:
        return points

    # find global peak (maximum file size)
    global_peak_idx = max(range(len(points)), key=lambda i: points[i][2])
    global_peak = points[global_peak_idx]

    # find x-axis maximum (latest file)
    x_max_idx = max(range(len(points)), key=lambda i: points[i][3])
    x_max_point = points[x_max_idx]

    # keep key points
    keep_indices = {0, len(points) - 1, global_peak_idx, x_max_idx}

    # calculate remaining points needed
    remaining_points = SAMPLING_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, x_max_point, points[-1]]

    # sort indices to find gaps
    sorted_keep_indices = sorted(keep_indices)

    # distribute points across gaps
    points_per_gap = remaining_points // (len(sorted_keep_indices) - 1)
    extra_points = remaining_points % (len(sorted_keep_indices) - 1)

    # sample from each gap
    for i in range(len(sorted_keep_indices) - 1):
        start_idx = sorted_keep_indices[i]
        end_idx = sorted_keep_indices[i + 1]
        gap_size = end_idx - start_idx - 1

        if gap_size <= 0:
            continue

        # points for current gap
        current_gap_points = points_per_gap
        if extra_points > 0:
            current_gap_points += 1
            extra_points -= 1

        if current_gap_points > 0:
            # random sampling
            available_indices = list(range(start_idx + 1, end_idx))
            sampled_indices = random.sample(available_indices, min(
                current_gap_points, len(available_indices)))
            keep_indices.update(sampled_indices)

    # return sorted points
    result = [points[i] for i in sorted(keep_indices)]
    return result


@file_sizes_bp.route('/file-sizes')
@check_and_reload_data()
def get_file_sizes():
    try:
        order = request.args.get('order', 'asc')
        file_type = request.args.get('type', 'all')
        if order not in ['asc', 'desc', 'created-time']:
            return jsonify({'error': 'Invalid order'}), 400
        if file_type not in ['temp', 'meta', 'buffer', 'task-created', 'transferred', 'all']:
            return jsonify({'error': 'Invalid file type'}), 400

        data = {}

        # collect file sizes
        data['file_sizes'] = []
        max_file_size_mb = 0
        for file in runtime_state.files.values():
            if len(file.transfers) == 0:
                continue
            file_name = file.filename
            file_size = file.size_mb
            if file_type != 'all':
                if file_type == 'temp' and not file_name.startswith('temp-'):
                    continue
                if file_type == 'meta' and not file_name.startswith('file-meta'):
                    continue
                if file_type == 'buffer' and not file_name.startswith('buffer-'):
                    continue
                if file_type == 'task-created' and len(file.producers) == 0:
                    continue
                if file_type == 'transferred' and len(file.transfers) == 0:
                    continue
            file_creation_time = float('inf')
            for transfer in file.transfers:
                file_creation_time = round(
                    min(file_creation_time, transfer.time_start_stage_in - runtime_state.MIN_TIME), 2)
            if file_creation_time == float('inf'):
                print(f"Warning: file {file_name} has no transfer")
            data['file_sizes'].append(
                (0, file_name, file_size, file_creation_time))
            max_file_size_mb = max(max_file_size_mb, file_size)

        # sort data
        df = pd.DataFrame(data['file_sizes'], columns=[
                          'file_idx', 'file_name', 'file_size', 'file_created_time'])
        df = df.sort_values(by=['file_created_time'])

        # set index and scale file size
        df['file_idx'] = range(1, len(df) + 1)
        y_unit, scale = get_unit_and_scale_by_max_file_size_mb(max_file_size_mb)
        df['file_size'] = df['file_size'] * scale

        # downsample data
        points = df.values.tolist()
        points = downsample_file_sizes(points)

        # d: [file_idx, file_name, file_size, file_creation_time]
        data['points'] = [[d[0], d[2]] for d in points]

        # set plotting parameters
        x_min = 1
        y_min = 0
        if len(points) == 0:
            x_max = 1
            y_max = 0
        else:
            x_max = len(df)
            y_max = max_file_size_mb * scale
        data['x_domain'] = [x_min, x_max]
        data['y_domain'] = [y_min, y_max]
        data['x_tick_values'] = compute_tick_values(data['x_domain'])
        data['y_tick_values'] = compute_tick_values(data['y_domain'])
        
        data['x_tick_formatter'] = d3_int_formatter()
        data['y_tick_formatter'] = d3_size_formatter(y_unit)

        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
