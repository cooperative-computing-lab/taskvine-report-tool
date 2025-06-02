from .utils import *
from flask import Blueprint, jsonify, make_response, current_app
import math
import pandas as pd
from io import StringIO

task_completion_percentiles_bp = Blueprint('task_completion_percentiles', __name__, url_prefix='/api')

def get_completion_percentile_points():
    tasks = current_app.config["RUNTIME_STATE"].tasks.values()
    finish_times = [
        (task.when_done or task.when_retrieved) - current_app.config["RUNTIME_STATE"].MIN_TIME
        for task in tasks
        if (task.when_done or task.when_retrieved)
    ]
    x_domain = list(range(1, 101))

    if not finish_times:
        return x_domain, []

    finish_times = sorted(finish_times)
    n = len(finish_times)

    points = []
    for p in range(1, 101):
        idx = int(math.ceil(p / 100 * n)) - 1
        idx = max(0, min(idx, n - 1))
        points.append([p, finish_times[idx]])

    return x_domain, points

@task_completion_percentiles_bp.route('/task-completion-percentiles')
@check_and_reload_data()
def get_task_completion_percentiles():
    try:
        x_domain, points = get_completion_percentile_points()

        if not points:
            return jsonify({
                'points': [],
                'x_domain': x_domain,
                'y_domain': [0, 0],
                'x_tick_values': compute_discrete_tick_values(x_domain),
                'y_tick_values': [0],
                'x_tick_formatter': d3_percentage_formatter(digits=0),
                'y_tick_formatter': d3_time_formatter()
            })

        y_max = max(y for _, y in points)
        y_domain = [0, y_max]

        return jsonify({
            'points': points,
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

@task_completion_percentiles_bp.route('/task-completion-percentiles/export-csv')
@check_and_reload_data()
def export_task_completion_percentiles_csv():
    try:
        x_domain, points = get_completion_percentile_points()

        if not points:
            return jsonify({'error': 'No task completion data available'}), 404

        df = pd.DataFrame(points, columns=["Percentile", "Completion Time"])
        df["Percentile (%)"] = df["Percentile"].astype(int)
        df["Completion Time (s)"] = df["Completion Time"].round(2)

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=task_completion_percentiles.csv"
        response.headers["Content-Type"] = "text/csv"
        return response
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_task_completion_percentiles_csv: {e}")
        return jsonify({'error': str(e)}), 500