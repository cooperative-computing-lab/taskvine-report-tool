from .utils import *
from flask import Blueprint, jsonify, current_app
import pandas as pd
import os

task_execution_time_bp = Blueprint(
    'task_execution_time', __name__, url_prefix='/api'
)

@task_execution_time_bp.route('/task-execution-time')
@check_and_reload_data()
def get_task_execution_time():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_task_execution_time

        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404

        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({'error': 'CSV is empty'}), 404

        # count completion status
        ran_to_completion_count = int(df['Ran to Completion'].sum())
        failed_count = int(len(df) - ran_to_completion_count)

        points = df[['Global Index', 'Execution Time', 'Task ID', 'Task Try ID', 'Ran to Completion']].values.tolist()
        x_domain, y_domain = compute_points_domain(points)

        return jsonify({
            'points': downsample_points(points),
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
