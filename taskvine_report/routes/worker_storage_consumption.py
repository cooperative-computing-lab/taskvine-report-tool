from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app, request
import pandas as pd

worker_storage_consumption_bp = Blueprint(
    'worker_storage_consumption', __name__, url_prefix='/api'
)

def aggregate_storage_data(df):
    df_indexed = df.set_index('Time (s)')
    
    df_filled = df_indexed.fillna(method='ffill').fillna(0)
    
    aggregated_series = df_filled.sum(axis=1)

    aggregated = aggregated_series.reset_index()
    aggregated.columns = ['Time (s)', 'Total Storage']
    
    return extract_points_from_df(aggregated, 'Time (s)', 'Total Storage')

@worker_storage_consumption_bp.route('/worker-storage-consumption')
@check_and_reload_data()
def get_worker_storage_consumption():
    try:
        accumulated = request.args.get('accumulated', 'false').lower() == 'true'
        
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_worker_storage_consumption)
        x_domain = get_current_time_domain()
        
        if accumulated:
            accumulated_points = aggregate_storage_data(df)
            max_storage = max(p[1] for p in accumulated_points) if accumulated_points else 0
            unit, scale = get_size_unit_and_scale(max_storage)
            if scale != 1:
                accumulated_points = [[t, s * scale] for t, s in accumulated_points]
            
            y_domain = extract_y_range_from_points(accumulated_points)
            
            return jsonify({
                'accumulated_data': downsample_points(accumulated_points),
                'x_domain': x_domain,
                'y_domain': y_domain,
                'x_tick_values': compute_linear_tick_values(x_domain),
                'y_tick_values': compute_linear_tick_values(y_domain),
                'x_tick_formatter': d3_time_formatter(),
                'y_tick_formatter': d3_size_formatter(unit),
            })
        else:
            storage_data = extract_series_points_dict(df, 'Time (s)')
            storage_data, size_unit = scale_storage_series_points(storage_data)
            y_domain = extract_y_range_from_series_points(storage_data)
            
            return jsonify({
                'storage_data': downsample_series_points(storage_data),
                'x_domain': x_domain,
                'y_domain': y_domain,
                'x_tick_values': compute_linear_tick_values(x_domain),
                'y_tick_values': compute_linear_tick_values(y_domain),
                'x_tick_formatter': d3_time_formatter(),
                'y_tick_formatter': d3_size_formatter(size_unit),
            })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_worker_storage_consumption: {e}")
        return jsonify({'error': str(e)}), 500
