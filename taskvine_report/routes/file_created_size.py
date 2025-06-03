from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

file_created_size_bp = Blueprint('file_created_size', __name__, url_prefix='/api')

@file_created_size_bp.route('/file-created-size')
@check_and_reload_data()
def get_file_created_size():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_file_created_size)
        points, unit = extract_size_points_from_df(df, 'Time (s)', 'Cumulative Size (MB)')
        x_domain = extract_x_range_from_points(points)
        y_domain = extract_y_range_from_points(points)

        return jsonify({
            'points': downsample_points(points),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_size_formatter(unit)
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_file_created_size: {e}")
        return jsonify({'error': str(e)}), 500
