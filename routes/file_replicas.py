from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_tick_values,
    d3_int_formatter,
    downsample_points,
    compute_discrete_tick_values
)
from flask import Blueprint, jsonify
import pandas as pd

file_replicas_bp = Blueprint('file_replicas', __name__, url_prefix='/api')

@file_replicas_bp.route('/file-replicas')
@check_and_reload_data()
def get_file_replicas():
    try:
        rows = []
        max_replicas = 0

        for file in runtime_state.files.values():
            if not file.transfers:
                continue

            fname = file.filename
            if not fname.startswith('temp-'):
                continue

            workers = set()
            for transfer in file.transfers:
                if transfer.time_stage_in:
                    workers.add(transfer.destination)
            
            num_replicas = len(workers)
            created_time = min((t.time_start_stage_in for t in file.transfers), default=float('inf')) - runtime_state.MIN_TIME
            created_time = round(created_time, 2) if created_time != float('inf') else float('inf')

            rows.append((0, fname, num_replicas, created_time))
            max_replicas = max(max_replicas, num_replicas)

        if not rows:
            return jsonify({
                'points': [],
                'file_idx_to_names': {},
                'x_domain': [1, 1],
                'y_domain': [0, 0],
                'x_tick_values': compute_tick_values([1, 1]),
                'y_tick_values': compute_tick_values([0, 0]),
                'x_tick_formatter': d3_int_formatter(),
                'y_tick_formatter': d3_int_formatter()
            })

        df = pd.DataFrame(rows, columns=['file_idx', 'file_name', 'num_replicas', 'created_time'])
        df = df.sort_values(by='created_time')
        df['file_idx'] = range(1, len(df) + 1)

        y_domain = sorted(df['num_replicas'].unique().tolist())
        downsampled = downsample_points(df.values.tolist(), SAMPLING_POINTS)

        print(f'y_tick_values: {compute_discrete_tick_values(y_domain)}')

        data = {
            'points': [[d[0], d[2]] for d in downsampled],
            'file_idx_to_names': {d[0]: d[1] for d in downsampled},
            'x_domain': [1, len(df)],
            'y_domain': y_domain,
            'x_tick_values': compute_tick_values([1, len(df)]),
            'y_tick_values': compute_discrete_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_int_formatter()
        }

        return jsonify(data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
