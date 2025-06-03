from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

worker_executing_tasks_bp = Blueprint('worker_executing_tasks', __name__, url_prefix='/api')

@worker_executing_tasks_bp.route('/worker-executing-tasks')
@check_and_reload_data()
def get_worker_executing_tasks():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_worker_executing_tasks)
        data = extract_series_points_dict(df, 'Time (s)')
        x_domain, y_domain = extract_xy_domains_from_series_points(data)

        return jsonify({
            'executing_tasks_data': downsample_series_points(data),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_worker_executing_tasks: {e}")
        return jsonify({'error': str(e)}), 500