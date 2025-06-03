from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

task_completion_percentiles_bp = Blueprint('task_completion_percentiles', __name__, url_prefix='/api')

@task_completion_percentiles_bp.route('/task-completion-percentiles')
@check_and_reload_data()
def get_task_completion_percentiles():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_task_completion_percentiles)
        points = extract_points_from_df(df, 'Percentile', 'Completion Time')
        x_domain = list(range(1, 101))
        y_domain = extract_y_range_from_points(points)

        return jsonify({
            'points': downsample_points(points),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_discrete_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_percentage_formatter(digits=0),
            'y_tick_formatter': d3_time_formatter()
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_completion_percentiles: {e}")
        return jsonify({'error': str(e)}), 500