from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

file_sizes_bp = Blueprint('file_sizes', __name__, url_prefix='/api')

def _empty_file_sizes_payload():
    x_domain = [0, 1]
    y_domain = [0, 1]
    return {
        'points': [],
        'x_domain': x_domain,
        'y_domain': y_domain,
        'x_tick_values': compute_linear_tick_values(x_domain),
        'y_tick_values': compute_linear_tick_values(y_domain),
        'x_tick_formatter': d3_int_formatter(),
        'y_tick_formatter': d3_size_formatter('MB'),
        'file_idx_to_names': {},
    }

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

    except ValueError as e:
        # Empty CSV is a valid state for some runs; return an empty dataset.
        if "CSV file is empty" in str(e):
            current_app.config["RUNTIME_STATE"].log_info(
                "file_sizes.csv is empty, returning empty response payload"
            )
            return jsonify(_empty_file_sizes_payload())
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_file_sizes: {e}")
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_file_sizes: {e}")
        return jsonify({'error': str(e)}), 500
