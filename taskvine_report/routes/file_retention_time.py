from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

file_retention_time_bp = Blueprint('file_retention_time', __name__, url_prefix='/api')

@file_retention_time_bp.route('/file-retention-time')
@check_and_reload_data()
def get_file_retention_time():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_retention_time)
        points = extract_points_from_df(df, 'File Index', 'Retention Time (s)')
        x_domain = extract_x_range_from_points(points)
        y_domain = extract_y_range_from_points(points)

        return jsonify({
            'points': downsample_points(points),
            'file_idx_to_names': dict(zip(df['File Index'], df['File Name'])),
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
