from .utils import *
from flask import Blueprint, jsonify, current_app
import pandas as pd
import os

worker_waiting_retrieval_tasks_bp = Blueprint('worker_waiting_retrieval_tasks', __name__, url_prefix='/api')

@worker_waiting_retrieval_tasks_bp.route('/worker-waiting-retrieval-tasks')
@check_and_reload_data()
def get_worker_waiting_retrieval_tasks():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_worker_waiting_retrieval_tasks
        
        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404
        
        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({'error': 'No valid worker waiting retrieval tasks data available'}), 404

        # Convert DataFrame to the expected format
        data = {}
        max_y = 0
        
        # Get all worker columns (exclude time column)
        worker_columns = [col for col in df.columns if col != 'time (s)']
        
        for worker_col in worker_columns:
            points = df[['time (s)', worker_col]].dropna().values.tolist()
            # Data is already compressed when CSV was generated
            data[worker_col] = points
            max_y = max(max_y, max(p[1] for p in points))

        x_domain = [0, float(current_app.config["RUNTIME_STATE"].MAX_TIME - current_app.config["RUNTIME_STATE"].MIN_TIME)]
        y_domain = [0, max(1.0, max_y)]

        return jsonify({
            'waiting_retrieval_tasks_data': data,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_worker_waiting_retrieval_tasks: {e}")
        return jsonify({'error': str(e)}), 500
