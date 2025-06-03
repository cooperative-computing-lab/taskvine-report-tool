from .utils import *
from flask import Blueprint, jsonify, Response, current_app
import pandas as pd
import os

worker_concurrency_bp = Blueprint('worker_concurrency', __name__, url_prefix='/api')

@worker_concurrency_bp.route('/worker-concurrency')
@check_and_reload_data()
def get_worker_concurrency():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_worker_concurrency

        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404

        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({'error': 'CSV is empty'}), 404

        points = df[['Time (s)', 'Active Workers (count)']].values.tolist()
        x_domain, y_domain = compute_points_domain(points)

        return jsonify({
            'points': downsample_points(points),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter()
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_worker_concurrency: {e}")
        return jsonify({'error': str(e)}), 500

