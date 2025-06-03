from flask import Blueprint, jsonify, current_app
from .utils import (
    compute_linear_tick_values,
    d3_int_formatter,
    compute_points_domain,
)

import pandas as pd
import os

task_dependents_bp = Blueprint('task_dependents', __name__, url_prefix='/api')

@task_dependents_bp.route('/task-dependents')
def get_task_dependents():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_task_dependents

        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404

        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({'error': 'CSV is empty'}), 404

        points = df[['Global Index', 'Dependent Count']].values.tolist()
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
