from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

worker_concurrency_bp = Blueprint('worker_concurrency', __name__, url_prefix='/api')

@worker_concurrency_bp.route('/worker-concurrency')
@check_and_reload_data()
def get_worker_concurrency():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_worker_concurrency)
        points = extract_points_from_df(df, 'Time (s)', 'Active Workers (count)')
        x_domain = extract_x_range_from_points(points)
        y_domain = extract_y_range_from_points(points)

        return jsonify({
            'points': downsample_points(points),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter()
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_worker_concurrency: {e}")
        return jsonify({'error': str(e)}), 500

