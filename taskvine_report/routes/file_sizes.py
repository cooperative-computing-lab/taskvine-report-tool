from .utils import *
from flask import Blueprint, jsonify, current_app
import pandas as pd
import os

file_sizes_bp = Blueprint('file_sizes', __name__, url_prefix='/api')

@file_sizes_bp.route('/file-sizes')
@check_and_reload_data()
def get_file_sizes():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_sizes
        
        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404
        
        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({
                'points': [],
                'file_idx_to_names': {},
                'x_domain': [1, 1],
                'y_domain': [0, 0],
                'x_tick_values': compute_linear_tick_values([1, 1]),
                'y_tick_values': compute_linear_tick_values([0, 0]),
                'x_tick_formatter': d3_int_formatter(),
                'y_tick_formatter': d3_size_formatter('MB'),
            })

        # Extract unit from column name
        size_column = [col for col in df.columns if col.startswith('Size (')][0]
        unit = size_column.split('(')[1].split(')')[0]

        points = df[['File Index', size_column]].values.tolist()
        points = [[x, round(y, 2)] for x, y in points]
        
        file_idx_to_names = {
            row['File Index']: row['File Name'] for _, row in df.iterrows()
        }

        x_domain, y_domain = compute_points_domain(points)
        downsampled_points = downsample_points(points)

        return jsonify({
            'points': downsampled_points,
            'file_idx_to_names': file_idx_to_names,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_size_formatter(unit),
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_file_sizes: {e}")
        return jsonify({'error': str(e)}), 500
