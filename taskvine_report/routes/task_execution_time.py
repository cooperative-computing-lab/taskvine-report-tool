from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

task_execution_time_bp = Blueprint(
    'task_execution_time', __name__, url_prefix='/api'
)

@task_execution_time_bp.route('/task-execution-time')
@check_and_reload_data()
def get_task_execution_time():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_task_execution_time)
        points = extract_points_from_df(df, 'Global Index', 'Execution Time', 'Task ID', 'Task Try ID', 'Ran to Completion')
        x_domain = extract_x_range_from_points(points, x_index=0)
        y_domain = extract_y_range_from_points(points, y_index=1)

        return jsonify({
            'points': downsample_points(points, y_index=1),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_time_formatter(),
            'ran_to_completion_count': int(df['Ran to Completion'].sum()),
            'failed_count': int(len(df) - int(df['Ran to Completion'].sum()))
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_execution_time: {e}")
        return jsonify({'error': str(e)}), 500
