from .utils import *
from flask import Blueprint, jsonify, current_app
import pandas as pd
import os

task_completion_percentiles_bp = Blueprint('task_completion_percentiles', __name__, url_prefix='/api')

@task_completion_percentiles_bp.route('/task-completion-percentiles')
@check_and_reload_data()
def get_task_completion_percentiles():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_task_completion_percentiles
        
        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404
        
        df = pd.read_csv(csv_path)
        if df.empty:
            x_domain = list(range(1, 101))
            return jsonify({
                'points': [],
                'x_domain': x_domain,
                'y_domain': [0, 0],
                'x_tick_values': compute_discrete_tick_values(x_domain),
                'y_tick_values': [0],
                'x_tick_formatter': d3_percentage_formatter(digits=0),
                'y_tick_formatter': d3_time_formatter()
            })

        points = df[['Percentile', 'Completion Time']].values.tolist()
        x_domain = list(range(1, 101))
        y_max = df['Completion Time'].max()
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