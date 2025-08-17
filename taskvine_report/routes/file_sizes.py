from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

file_sizes_bp = Blueprint('file_sizes', __name__, url_prefix='/api')

@file_sizes_bp.route('/file-sizes')
@check_and_reload_data()
def get_file_sizes():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_sizes)
        # find the size column by looking for a column that ends with unit suffix
        size_col = next(col for col in df.columns if col.startswith('file_size_'))
        unit = size_col.split('_')[-1].upper()  # Extract unit from column name (e.g., 'file_size_mb' -> 'MB')
        points = extract_points_from_df(df, 'file_idx', size_col)
        x_domain = extract_x_range_from_points(points)
        y_domain = extract_y_range_from_points(points)

        return jsonify({
            'points': downsample_points(points, target_point_count=current_app.config["DOWNSAMPLE_POINTS"]),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_size_formatter(unit),
            'file_idx_to_names': dict(zip(df['file_idx'], df['file_name'])),
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_file_sizes: {e}")
        return jsonify({'error': str(e)}), 500
