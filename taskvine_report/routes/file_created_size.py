from .utils import *
from flask import Blueprint, jsonify, current_app
import pandas as pd
import os

file_created_size_bp = Blueprint('file_created_size', __name__, url_prefix='/api')

@file_created_size_bp.route('/file-created-size')
@check_and_reload_data()
def get_file_created_size():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_created_size
        
        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404
        
        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({'error': 'CSV is empty'}), 404

        raw_points = df[['Time (s)', 'Cumulative Size (MB)']].values.tolist()
        x_max = df['Time (s)'].max()
        y_max = df['Cumulative Size (MB)'].max()

        unit, scale = get_unit_and_scale_by_max_file_size_mb(y_max if pd.notna(y_max) else 0)
        scaled_points = [[x, y * scale] for x, y in raw_points]

        x_domain = [0, float(x_max) if pd.notna(x_max) else 1.0]
        y_domain = [0, y_max * scale if pd.notna(y_max) else 0]

        return jsonify({
            'points': downsample_points(scaled_points),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_size_formatter(unit)
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_file_created_size: {e}")
        return jsonify({'error': str(e)}), 500
