from .utils import *
from flask import Blueprint, jsonify, make_response, current_app
from io import StringIO
import pandas as pd

file_sizes_bp = Blueprint('file_sizes', __name__, url_prefix='/api')

def get_file_size_points(files, base_time):
    rows = []
    max_size = 0

    base_rows = get_task_produced_files(files, base_time)
    
    for file_idx, fname, created_time in base_rows:
        file = files[fname]
        size = file.size_mb
        if size is None:
            continue
        rows.append((file_idx, fname, size, created_time))
        max_size = max(max_size, size)

    if not rows:
        return [], {}, 'MB', 1

    df = pd.DataFrame(rows, columns=['file_idx', 'file_name', 'file_size', 'created_time'])
    df = df.sort_values(by='created_time')
    df['file_idx'] = range(1, len(df) + 1)

    unit, scale = get_unit_and_scale_by_max_file_size_mb(max_size)
    df['file_size'] = (df['file_size'] * scale)

    points = df[['file_idx', 'file_size']].values.tolist()
    file_idx_to_names = {
        row['file_idx']: row['file_name'] for _, row in df.iterrows()
    }

    return points, file_idx_to_names, unit, scale

@file_sizes_bp.route('/file-sizes')
@check_and_reload_data()
def get_file_sizes():
    try:
        points, file_idx_to_names, unit, _ = get_file_size_points(current_app.config["RUNTIME_STATE"].files, current_app.config["RUNTIME_STATE"].MIN_TIME)
        # round points to 2 decimal places
        points = [[x, round(y, 2)] for x, y in points]

        if not points:
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

        x_domain, y_domain = compute_points_domain(points)
        downsampled_points = downsample_points(points)

        return jsonify({
            'points': downsampled_points,
            'file_idx_to_names': file_idx_to_names,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_size_formatter(unit),
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_file_sizes: {e}")
        return jsonify({'error': str(e)}), 500

@file_sizes_bp.route('/file-sizes/export-csv')
@check_and_reload_data()
def export_file_sizes_csv():
    try:
        points, file_idx_to_names, unit, _ = get_file_size_points(current_app.config["RUNTIME_STATE"].files, current_app.config["RUNTIME_STATE"].MIN_TIME)
        if not points:
            return jsonify({'error': 'No file size data available'}), 404

        df = pd.DataFrame(points, columns=['File Index', f'Size ({unit})'])
        df['File Name'] = df['File Index'].map(file_idx_to_names)
        df = df[['File Index', 'File Name', f'Size ({unit})']]

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=file_sizes.csv"
        response.headers["Content-Type"] = "text/csv"
        return response

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_file_sizes_csv: {e}")
        return jsonify({'error': str(e)}), 500
