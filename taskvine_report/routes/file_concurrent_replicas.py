from .utils import *
from flask import Blueprint, jsonify, make_response, current_app
from io import StringIO
import pandas as pd

file_concurrent_replicas_bp = Blueprint('file_concurrent_replicas', __name__, url_prefix='/api')

def get_file_max_concurrent_replica_points():
    base_rows = get_task_produced_files(current_app.config["RUNTIME_STATE"].files, current_app.config["RUNTIME_STATE"].MIN_TIME)
    rows = []

    for file_idx, fname, created_time in base_rows:
        file = current_app.config["RUNTIME_STATE"].files[fname]
        intervals = [
            (t.time_stage_in, t.time_stage_out)
            for t in file.transfers
            if t.time_stage_in and t.time_stage_out
        ]
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
            for _, delta in events:
                count += delta
                max_simul = max(max_simul, count)

        rows.append((file_idx, fname, max_simul, created_time))

    if not rows:
        return [], {}, [1, 1], [0, 0]

    df = pd.DataFrame(rows, columns=['file_idx', 'file_name', 'max_simul_replicas', 'created_time'])
    df = df.sort_values(by='created_time')
    df['file_idx'] = range(1, len(df) + 1)

    file_idx_to_names = dict(zip(df['file_idx'], df['file_name']))
    points = df[['file_idx', 'max_simul_replicas']].values.tolist()

    x_domain, y_domain = compute_points_domain(points)
    return points, file_idx_to_names, x_domain, y_domain, df


@file_concurrent_replicas_bp.route('/file-concurrent-replicas')
@check_and_reload_data()
def get_file_concurrent_replicas():
    try:
        points, file_idx_to_names, x_domain, y_domain, _ = get_file_max_concurrent_replica_points()
        points = [[x, round(y, 2)] for x, y in points]

        if not points:
            return jsonify({
                'points': [],
                'file_idx_to_names': {},
                'x_domain': x_domain,
                'y_domain': y_domain,
                'x_tick_values': compute_linear_tick_values(x_domain),
                'y_tick_values': compute_linear_tick_values(y_domain),
                'x_tick_formatter': d3_int_formatter(),
                'y_tick_formatter': d3_int_formatter()
            })

        downsampled_points = downsample_points(points)

        return jsonify({
            'points': downsampled_points,
            'file_idx_to_names': file_idx_to_names,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain, round_digits=0, num_ticks=10),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_int_formatter()
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_file_concurrent_replicas: {e}")
        return jsonify({'error': str(e)}), 500

@file_concurrent_replicas_bp.route('/file-concurrent-replicas/export-csv')
@check_and_reload_data()
def export_file_concurrent_replicas_csv():
    try:
        _, _, _, _, df = get_file_max_concurrent_replica_points()
        if df.empty:
            return jsonify({'error': 'No concurrent replica data'}), 404

        df = df[['file_idx', 'file_name', 'max_simul_replicas']]
        df.columns = ['File Index', 'File Name', 'Max Concurrent Replicas (count)']

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=file_concurrent_replicas.csv'
        response.headers['Content-Type'] = 'text/csv'
        return response

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_file_concurrent_replicas_csv: {e}")
        return jsonify({'error': str(e)}), 500
