from .utils import *

from flask import Blueprint, jsonify, make_response, current_app
import pandas as pd
from io import StringIO

task_response_time_bp = Blueprint(
    'task_response_time', __name__, url_prefix='/api'
)

def get_response_time_points():
    if not current_app.config["RUNTIME_STATE"].task_stats:
        return []

    return [
        [row['global_idx'], row['task_response_time'], row['task_id'], row['task_try_id'], row['was_dispatched']]
        for row in current_app.config["RUNTIME_STATE"].task_stats
    ]

@task_response_time_bp.route('/task-response-time')
@check_and_reload_data()
def get_task_response_time():
    try:
        raw_points = get_response_time_points()

        if not raw_points:
            return jsonify({'error': 'No valid response time found'}), 404

        # count original points before downsampling
        dispatched_count = sum(1 for p in raw_points if p[4])
        undispatched_count = sum(1 for p in raw_points if not p[4])

        x_domain, y_domain = compute_points_domain(raw_points)

        return jsonify({
            'points': downsample_points(raw_points),
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


@task_response_time_bp.route('/task-response-time/export-csv')
@check_and_reload_data()
def export_task_response_time_csv():
    try:
        raw_points = get_response_time_points()

        if not raw_points:
            return jsonify({'error': 'No valid response time found'}), 404

        df = pd.DataFrame(raw_points, columns=["Global Index", "Response Time", "Task ID", "Try ID", "Was Dispatched"])

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=task_response_time.csv"
        response.headers["Content-Type"] = "text/csv"
        return response

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_task_response_time_csv: {e}")
        return jsonify({'error': str(e)}), 500
