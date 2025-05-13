from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_int_formatter,
    downsample_points,
    compute_points_domain,
    get_task_produced_files
)
from flask import Blueprint, jsonify
import pandas as pd

file_concurrent_replicas_bp = Blueprint('file_concurrent_replicas', __name__, url_prefix='/api')

@file_concurrent_replicas_bp.route('/file-concurrent-replicas')
@check_and_reload_data()
def get_file_concurrent_replicas():
    try:
        rows = []
        
        base_rows = get_task_produced_files(runtime_state.files, runtime_state.MIN_TIME)
        
        for file_idx, fname, created_time in base_rows:
            file = runtime_state.files[fname]
            
            intervals = []
            for transfer in file.transfers:
                if transfer.time_stage_in and transfer.time_stage_out:
                    intervals.append((transfer.time_stage_in, transfer.time_stage_out))
            if not intervals:
                max_simul = 0
            else:
                events = []
                for start, end in intervals:
                    events.append((start, 1))
                    events.append((end, -1))
                events.sort()
                count = 0
                max_simul = 0
                for t, delta in events:
                    count += delta
                    max_simul = max(max_simul, count)
            
            rows.append((file_idx, fname, max_simul, created_time))

        if not rows:
            return jsonify({
                'points': [],
                'file_idx_to_names': {},
                'x_domain': [1, 1],
                'y_domain': [0, 0],
                'x_tick_values': compute_linear_tick_values([1, 1]),
                'y_tick_values': compute_linear_tick_values([0, 0]),
                'x_tick_formatter': d3_int_formatter(),
                'y_tick_formatter': d3_int_formatter()
            })

        df = pd.DataFrame(rows, columns=['file_idx', 'file_name', 'max_simul_replicas', 'created_time'])
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
            'y_tick_values': compute_linear_tick_values(y_domain, round_digits=0, num_ticks=10),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_int_formatter()
        }

        return jsonify(data)

    except Exception as e:
        runtime_state.log_error(f"Error in get_file_concurrent_replicas: {e}")
        return jsonify({'error': str(e)}), 500
