from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

task_response_time_bp = Blueprint(
    'task_response_time', __name__, url_prefix='/api'
)

@task_response_time_bp.route('/task-response-time')
@check_and_reload_data()
def get_task_response_time():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_task_response_time)
        points = extract_points_from_df(df, 'Global Index', 'Response Time', 'Task ID', 'Task Try ID', 'Was Dispatched')
        x_domain = extract_x_range_from_points(points, x_index=0)
        y_domain = extract_y_range_from_points(points, y_index=1)

        # Get dispatch statistics from preloaded metadata
        metadata = current_app.config["RUNTIME_STATE"].metadata
        dispatched_count = metadata.get('dispatched_tasks', 0)
        undispatched_count = metadata.get('undispatched_tasks', 0)

        return jsonify({
            'points': downsample_points(points, y_index=1),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_time_formatter(),
            'dispatched_count': dispatched_count,
            'undispatched_count': undispatched_count
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_response_time: {e}")
        return jsonify({'error': str(e)}), 500
