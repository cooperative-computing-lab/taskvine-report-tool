from flask import Blueprint, jsonify, current_app
import pandas as pd
import os

from .utils import (
    compute_linear_tick_values,
    d3_int_formatter,
    compute_points_domain,
)

task_dependencies_bp = Blueprint('task_dependencies', __name__, url_prefix='/api')

@task_dependencies_bp.route('/task-dependencies')
def get_task_dependencies():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_task_dependencies

        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404

        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({'error': 'CSV is empty'}), 404

        points = df[['Global Index', 'Dependency Count']].values.tolist()
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
