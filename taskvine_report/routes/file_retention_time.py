from .utils import *
from flask import Blueprint, jsonify, current_app
import pandas as pd
import os

file_retention_time_bp = Blueprint('file_retention_time', __name__, url_prefix='/api')

@file_retention_time_bp.route('/file-retention-time')
@check_and_reload_data()
def get_file_retention_time():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_retention_time

        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404

        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({'error': 'CSV is empty'}), 404

        file_idx_to_names = dict(zip(df['File Index'], df['File Name']))
        points = df[['File Index', 'Retention Time (s)']].values.tolist()
        points = [[x, round(y, 2)] for x, y in points]

        x_domain, y_domain = compute_points_domain(points)

        return jsonify({
            'points': downsample_points(points),
            'file_idx_to_names': file_idx_to_names,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_time_formatter()
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_file_retention_time: {e}")
        return jsonify({'error': str(e)}), 500
