from .utils import *
from flask import Blueprint, jsonify, current_app
import pandas as pd
import os

task_retrieval_time_bp = Blueprint('task_retrieval_time', __name__, url_prefix='/api')

@task_retrieval_time_bp.route('/task-retrieval-time')
@check_and_reload_data()
def get_task_retrieval_time():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_task_retrieval_time

        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404

        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({'error': 'CSV is empty'}), 404

        points = df[['Global Index', 'Retrieval Time', 'Task ID', 'Task Try ID']].values.tolist()
        x_domain, y_domain = compute_points_domain(points)

        return jsonify({
            'points': downsample_points(points),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_time_formatter()
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_retrieval_time: {e}")
        return jsonify({'error': str(e)}), 500