from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_int_formatter,
    downsample_points,
    compute_points_domain,
    d3_time_formatter,
    get_task_produced_files
)
from flask import Blueprint, jsonify
import pandas as pd

file_retention_time_bp = Blueprint('file_retention_time', __name__, url_prefix='/api')

@file_retention_time_bp.route('/file-retention-time')
@check_and_reload_data()
def get_file_retention_time():
    try:
        rows = []
        
        base_rows = get_task_produced_files(runtime_state.files, runtime_state.MIN_TIME)
        
        for file_idx, fname, created_time in base_rows:
            file = runtime_state.files[fname]
            
            first_stage_in = min((t.time_start_stage_in for t in file.transfers), default=float('inf'))
            last_stage_out = max((t.time_stage_out for t in file.transfers), default=float('-inf'))
            
            if first_stage_in == float('inf') or last_stage_out == float('-inf'):
                continue

            retention_time = round(last_stage_out - first_stage_in, 2)
            rows.append((file_idx, fname, retention_time, created_time))

        if not rows:
            return jsonify({
                'points': [],
                'file_idx_to_names': {},
                'x_domain': [1, 1],
                'y_domain': [0, 0],
                'x_tick_values': compute_linear_tick_values([1, 1]),
                'y_tick_values': compute_linear_tick_values([0, 0]),
                'x_tick_formatter': d3_int_formatter(),
                'y_tick_formatter': d3_time_formatter()
            })

        df = pd.DataFrame(rows, columns=['file_idx', 'file_name', 'retention_time', 'created_time'])
        df = df.sort_values(by='created_time')
        df['file_idx'] = range(1, len(df) + 1)

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
            'y_tick_formatter': d3_time_formatter()
        }

        return jsonify(data)

    except Exception as e:
        runtime_state.log_error(f"Error in get_file_retention_time: {e}")
        return jsonify({'error': str(e)}), 500
