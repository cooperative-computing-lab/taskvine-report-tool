from src.utils import *
from flask import Blueprint, jsonify, current_app

task_completion_percentiles_bp = Blueprint('task_completion_percentiles', __name__, url_prefix='/api')

@task_completion_percentiles_bp.route('/task-completion-percentiles')
@check_and_reload_data()
def get_task_completion_percentiles():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_task_completion_percentiles)
        percentile_time_points = extract_points_from_df(df, 'Percentile', 'Completion Time')
        points = [(completion_time, percentile) for percentile, completion_time in percentile_time_points]
        max_completion_time = max((point[0] for point in points), default=1.0)
        x_domain = [0.0, max(1.0, max_completion_time)]
        y_domain = [0, 100]

        return jsonify({
            'points': downsample_points(points, target_point_count=current_app.config["DOWNSAMPLE_POINTS"]),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': [0, 25, 50, 75, 100],
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_percentage_formatter(digits=0)
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_completion_percentiles: {e}")
        return jsonify({'error': str(e)}), 500