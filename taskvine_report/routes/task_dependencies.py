from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

task_dependencies_bp = Blueprint('task_dependencies', __name__, url_prefix='/api')

@task_dependencies_bp.route('/task-dependencies')
def get_task_dependencies():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_task_dependencies)
        points = extract_points_from_df(df, 'Global Index', 'Dependency Count')
        x_domain = extract_x_range_from_points(points)
        y_domain = extract_y_range_from_points(points)

        return jsonify({
            'points': downsample_points(points),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain, round_digits=0, num_ticks=10),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_int_formatter()
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_dependencies: {e}")
        return jsonify({'error': str(e)}), 500
