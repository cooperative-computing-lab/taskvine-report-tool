from .utils import *
import pandas as pd
import os
from flask import Blueprint, jsonify, current_app

task_concurrency_bp = Blueprint(
    'task_concurrency', __name__, url_prefix='/api')

@task_concurrency_bp.route('/task-concurrency')
@check_and_reload_data()
def get_task_concurrency():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_task_concurrency

        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404

        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({'error': 'CSV is empty'}), 404

        phase_data = {}
        max_y = 0
        
        for phase in ['Waiting', 'Committing', 'Executing', 'Retrieving', 'Done']:
            points = df[['Time (s)', phase]].dropna().values.tolist()
            # Data is already compressed when CSV was generated
            phase_key = f"tasks_{phase.lower()}"
            phase_data[phase_key] = points
            
            if points:
                max_y = max(max_y, max(p[1] for p in points))

        data = {
            **phase_data,
            'x_domain': [0, current_app.config["RUNTIME_STATE"].MAX_TIME - current_app.config["RUNTIME_STATE"].MIN_TIME],
            'y_domain': [0, max_y],
            'x_tick_values': compute_linear_tick_values([0, current_app.config["RUNTIME_STATE"].MAX_TIME - current_app.config["RUNTIME_STATE"].MIN_TIME]),
            'y_tick_values': compute_linear_tick_values([0, max_y]),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        }

        return jsonify(data)

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_concurrency: {e}")
        return jsonify({'error': str(e)}), 500
