from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

file_created_size_bp = Blueprint('file_created_size', __name__, url_prefix='/api')

def _empty_file_created_size_payload():
    x_domain = get_current_time_domain()
    y_domain = [0, 1]
    return {
        'points': [],
        'x_domain': x_domain,
        'y_domain': y_domain,
        'x_tick_values': compute_linear_tick_values(x_domain),
        'y_tick_values': compute_linear_tick_values(y_domain),
        'x_tick_formatter': d3_time_formatter(),
        'y_tick_formatter': d3_size_formatter("MB")
    }

@file_created_size_bp.route('/file-created-size')
@check_and_reload_data()
def get_file_created_size():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_file_created_size)

        if 'time' not in df.columns:
            current_app.config["RUNTIME_STATE"].log_info(
                "file_created_size.csv missing time column, returning empty payload"
            )
            return jsonify(_empty_file_created_size_payload())

        if 'cumulative_size_mb' in df.columns:
            points, unit = extract_size_points_from_df(df, 'time', 'cumulative_size_mb')
        elif 'delta_size_mb' in df.columns:
            # Backward compatibility for datasets that only contain per-time delta values.
            tmp = (
                df[['time', 'delta_size_mb']]
                .dropna()
                .assign(time=lambda t: pd.to_numeric(t['time'], errors='coerce'))
                .dropna()
                .groupby('time', as_index=False)['delta_size_mb'].sum()
                .sort_values('time')
            )
            tmp['cumulative_size_mb'] = tmp['delta_size_mb'].cumsum().clip(lower=0)
            points, unit = extract_size_points_from_df(tmp, 'time', 'cumulative_size_mb')
        else:
            current_app.config["RUNTIME_STATE"].log_info(
                "file_created_size.csv missing cumulative/delta size columns, returning empty payload"
            )
            return jsonify(_empty_file_created_size_payload())
        
        x_domain = get_current_time_domain()
        y_domain = extract_y_range_from_points(points)

        return jsonify({
            'points': downsample_points(points, target_point_count=current_app.config["DOWNSAMPLE_POINTS"]),
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
