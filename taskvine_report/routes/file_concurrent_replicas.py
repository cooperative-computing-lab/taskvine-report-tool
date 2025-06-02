from .utils import *
from flask import Blueprint, jsonify, make_response, current_app
from io import StringIO
import pandas as pd

file_concurrent_replicas_bp = Blueprint('file_concurrent_replicas', __name__, url_prefix='/api')

@file_concurrent_replicas_bp.route('/file-concurrent-replicas')
@check_and_reload_data()
def get_file_concurrent_replicas():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_concurrent_replicas

        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404

        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({'error': 'CSV is empty'}), 404

        file_idx_to_names = dict(zip(df['File Index'], df['File Name']))
        points = df[['File Index', 'Max Concurrent Replicas (count)']].values.tolist()
        points = [[x, round(y, 2)] for x, y in points]

        x_domain, y_domain = compute_points_domain(points)
        downsampled_points = downsample_points(points)

        return jsonify({
            'points': downsampled_points,
            'file_idx_to_names': file_idx_to_names,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain, round_digits=0, num_ticks=10),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_int_formatter()
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in /file-concurrent-replicas: {e}")
        return jsonify({'error': str(e)}), 500