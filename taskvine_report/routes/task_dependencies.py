from flask import Blueprint, jsonify, make_response, current_app
import pandas as pd
from io import StringIO

from .utils import (
    compute_linear_tick_values,
    d3_int_formatter,
    compute_points_domain,
)

task_dependencies_bp = Blueprint('task_dependencies', __name__, url_prefix='/api')

def get_dependency_points():
    if not current_app.config["RUNTIME_STATE"].task_stats:
        return []

    return [
        [row['global_idx'], row['dependency_count']]
        for row in current_app.config["RUNTIME_STATE"].task_stats
    ]


@task_dependencies_bp.route('/task-dependencies')
def get_task_dependencies():
    try:
        points = get_dependency_points()

        if not points:
            return jsonify({'error': 'No dependency data available'}), 404

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
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_dependencies: {e}")
        return jsonify({'error': str(e)}), 500


@task_dependencies_bp.route('/task-dependencies/export-csv')
def export_task_dependencies_csv():
    try:
        points = get_dependency_points()

        if not points:
            return jsonify({'error': 'No dependency data available'}), 404

        df = pd.DataFrame(points, columns=["Task ID", "Dependency Count"])

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=task_dependency_count.csv"
        response.headers["Content-Type"] = "text/csv"
        return response
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_task_dependencies_csv: {e}")
        return jsonify({'error': str(e)}), 500
