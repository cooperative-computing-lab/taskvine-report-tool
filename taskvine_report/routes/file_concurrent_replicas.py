from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

file_concurrent_replicas_bp = Blueprint('file_concurrent_replicas', __name__, url_prefix='/api')

@file_concurrent_replicas_bp.route('/file-concurrent-replicas')
@check_and_reload_data()
def get_file_concurrent_replicas():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_file_concurrent_replicas)
        points = extract_points_from_df(df, 'File Index', 'Max Concurrent Replicas (count)')
        x_domain = extract_x_range_from_points(points)
        y_domain = extract_y_range_from_points(points)

        return jsonify({
            'points': downsample_points(points),
            'file_idx_to_names': dict(zip(df['File Index'], df['File Name'])),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain, round_digits=0, num_ticks=10),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_int_formatter()
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in /file-concurrent-replicas: {e}")
        return jsonify({'error': str(e)}), 500