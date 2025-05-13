from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    get_unit_and_scale_by_max_file_size_mb,
    d3_size_formatter,
    compute_linear_tick_values,
    d3_int_formatter,
    downsample_points,
    compute_points_domain,
    get_task_produced_files
)
from flask import Blueprint, jsonify
import pandas as pd

file_sizes_bp = Blueprint('file_sizes', __name__, url_prefix='/api')

@file_sizes_bp.route('/file-sizes')
@check_and_reload_data()
def get_file_sizes():
    try:
        rows = []
        max_size = 0

        base_rows = get_task_produced_files(runtime_state.files, runtime_state.MIN_TIME)
        
        for file_idx, fname, created_time in base_rows:
            file = runtime_state.files[fname]
            size = file.size_mb
            if size is None:
                continue
            rows.append((file_idx, fname, size, created_time))
            max_size = max(max_size, size)

        if not rows:
            return jsonify({
                'points': [],
                'file_idx_to_names': {},
                'x_domain': [1, 1],
                'y_domain': [0, 0],
                'x_tick_values': compute_linear_tick_values([1, 1]),
                'y_tick_values': compute_linear_tick_values([0, 0]),
                'x_tick_formatter': d3_int_formatter(),
                'y_tick_formatter': d3_size_formatter('MB'),
            })

        df = pd.DataFrame(rows, columns=['file_idx', 'file_name', 'file_size', 'created_time'])
        df = df.sort_values(by='created_time')
        df['file_idx'] = range(1, len(df) + 1)

        unit, scale = get_unit_and_scale_by_max_file_size_mb(max_size)
        df['file_size'] *= scale

        file_idx_to_names = {row[0]: row[1] for row in df[['file_idx', 'file_name']].values.tolist()}

        points = [[d[0], d[2]] for d in df.values.tolist()]
        x_domain, y_domain = compute_points_domain(points)
        downsampled_points = downsample_points(points, SAMPLING_POINTS)

        data = {
            'points': downsampled_points,
            'file_idx_to_names': file_idx_to_names,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_size_formatter(unit),
        }

        return jsonify(data)

    except Exception as e:
        runtime_state.log_error(f"Error in get_file_sizes: {e}")
        return jsonify({'error': str(e)}), 500
