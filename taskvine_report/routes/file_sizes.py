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
        if "file_idx" not in df.columns or "file_name" not in df.columns:
            current_app.config["RUNTIME_STATE"].log_info(
                "file_sizes.csv missing required columns (file_idx/file_name), returning empty payload"
            )
            return jsonify(_empty_file_sizes_payload())

        # Prefer unit-tagged column (file_size_mb/file_size_gb/...), fallback to legacy file_size.
        size_col = next((col for col in df.columns if col.startswith('file_size_')), None)
        if size_col is None and "file_size" in df.columns:
            size_col = "file_size"
            unit = "MB"
        elif size_col is None:
            current_app.config["RUNTIME_STATE"].log_info(
                "file_sizes.csv missing size columns, returning empty payload"
            )
            return jsonify(_empty_file_sizes_payload())
        else:
            unit = size_col.split('_')[-1].upper()

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
