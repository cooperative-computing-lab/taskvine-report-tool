from .utils import *

from flask import Blueprint, jsonify, make_response, current_app
import pandas as pd
from io import StringIO

task_execution_time_bp = Blueprint(
    'task_execution_time', __name__, url_prefix='/api'
)

def get_execution_points():
    if not current_app.config["RUNTIME_STATE"].task_stats:
        return []

    return [
        [row['global_idx'], row['task_execution_time'], row['task_id'], row['task_try_id'], row['ran_to_completion']]
        for row in current_app.config["RUNTIME_STATE"].task_stats
    ]

@task_execution_time_bp.route('/task-execution-time')
@check_and_reload_data()
def get_task_execution_time():
    try:
        raw_points = get_execution_points()

        if not raw_points:
            return jsonify({'error': 'No completed tasks available'}), 404

        # count original points before downsampling
        ran_to_completion_count = sum(1 for p in raw_points if p[4])
        failed_count = sum(1 for p in raw_points if not p[4])

        x_domain, y_domain = compute_points_domain(raw_points)

        return jsonify({
            'points': downsample_points(raw_points),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_time_formatter(),
            'ran_to_completion_count': ran_to_completion_count,
            'failed_count': failed_count
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_execution_time: {e}")
        return jsonify({'error': str(e)}), 500


@task_execution_time_bp.route('/task-execution-time/export-csv')
@check_and_reload_data()
def export_task_execution_time_csv():
    try:
        raw_points = get_execution_points()

        if not raw_points:
            return jsonify({'error': 'No completed tasks available'}), 404

        df = pd.DataFrame(raw_points, columns=["Global Index", "Execution Time", "Task ID", "Task Try ID", "Ran to Completion"])

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=task_execution_time.csv"
        response.headers["Content-Type"] = "text/csv"
        return response

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_task_execution_time_csv: {e}")
        return jsonify({'error': str(e)}), 500
