from .utils import *
from flask import Blueprint, jsonify, make_response, current_app
from io import StringIO
import pandas as pd

file_retention_time_bp = Blueprint('file_retention_time', __name__, url_prefix='/api')

def get_file_retention_time_points():
    base_rows = get_task_produced_files(current_app.config["RUNTIME_STATE"].files, current_app.config["RUNTIME_STATE"].MIN_TIME)
    rows = []

    for file_idx, fname, created_time in base_rows:
        file = current_app.config["RUNTIME_STATE"].files[fname]
        first_stage_in = min((t.time_start_stage_in for t in file.transfers), default=float('inf'))
        last_stage_out = max((t.time_stage_out for t in file.transfers), default=float('-inf'))
        if first_stage_in == float('inf') or last_stage_out == float('-inf'):
            continue
        retention_time = last_stage_out - first_stage_in
        rows.append((file_idx, fname, retention_time, created_time))

    if not rows:
        return [], {}, [1, 1], [0, 0], pd.DataFrame()

    df = pd.DataFrame(rows, columns=['file_idx', 'file_name', 'retention_time', 'created_time'])
    df = df.sort_values(by='created_time')
    df['file_idx'] = range(1, len(df) + 1)

    file_idx_to_names = dict(zip(df['file_idx'], df['file_name']))
    points = df[['file_idx', 'retention_time']].round(2).values.tolist()
    x_domain, y_domain = compute_points_domain(points)

    return points, file_idx_to_names, x_domain, y_domain, df

@file_retention_time_bp.route('/file-retention-time')
@check_and_reload_data()
def get_file_retention_time():
    try:
        points, file_idx_to_names, x_domain, y_domain, _ = get_file_retention_time_points()

        if not points:
            return jsonify({
                'points': [],
                'file_idx_to_names': {},
                'x_domain': x_domain,
                'y_domain': y_domain,
                'x_tick_values': compute_linear_tick_values(x_domain),
                'y_tick_values': compute_linear_tick_values(y_domain),
                'x_tick_formatter': d3_int_formatter(),
                'y_tick_formatter': d3_time_formatter()
            })

        return jsonify({
            'points': downsample_points(points),
            'file_idx_to_names': file_idx_to_names,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_time_formatter()
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_file_retention_time: {e}")
        return jsonify({'error': str(e)}), 500

@file_retention_time_bp.route('/file-retention-time/export-csv')
@check_and_reload_data()
def export_file_retention_time_csv():
    try:
        _, _, _, _, df = get_file_retention_time_points()
        if df.empty:
            return jsonify({'error': 'No file retention data'}), 404

        df = df[['file_idx', 'file_name', 'retention_time']]
        df.columns = ['File Index', 'File Name', 'Retention Time (s)']

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=file_retention_time.csv'
        response.headers['Content-Type'] = 'text/csv'
        return response

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_file_retention_time_csv: {e}")
        return jsonify({'error': str(e)}), 500
