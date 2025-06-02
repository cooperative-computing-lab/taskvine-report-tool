from flask import Blueprint, jsonify, make_response, current_app
from .utils import (
    compute_linear_tick_values,
    d3_int_formatter,
    compute_points_domain,
)

import pandas as pd
from io import StringIO
from flask import make_response

task_dependents_bp = Blueprint('task_dependents', __name__, url_prefix='/api')

def get_dependent_points():
    if not current_app.config["RUNTIME_STATE"].task_stats:
        return []

    return [
        [row['global_idx'], row['dependent_count']]
        for row in current_app.config["RUNTIME_STATE"].task_stats
    ]

@task_dependents_bp.route('/task-dependents')
def get_task_dependents():
    try:
        points = get_dependent_points()

        if not points:
            return jsonify({'error': 'No dependent data available'}), 404

        x_domain, y_domain = compute_points_domain(points)

        return jsonify({
            'points': points,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain, round_digits=0, num_ticks=10),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_int_formatter()
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_dependents: {e}")
        return jsonify({'error': str(e)}), 500

@task_dependents_bp.route('/task-dependents/export-csv')
def export_task_dependents_csv():
    try:
        points = get_dependent_points()

        if not points:
            return jsonify({'error': 'No dependent data available'}), 404

        df = pd.DataFrame(points, columns=["Task ID", "Dependent Count"])

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=task_dependent_count.csv"
        response.headers["Content-Type"] = "text/csv"
        return response
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_task_dependents_csv: {e}")
        return jsonify({'error': str(e)}), 500
