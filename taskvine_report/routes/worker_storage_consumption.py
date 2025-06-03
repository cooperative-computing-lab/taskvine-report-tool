from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

worker_storage_consumption_bp = Blueprint(
    'worker_storage_consumption', __name__, url_prefix='/api'
)

@worker_storage_consumption_bp.route('/worker-storage-consumption')
@check_and_reload_data()
def get_worker_storage_consumption():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_worker_storage_consumption)
        storage_data = extract_series_points_dict(df, 'Time (s)')
        storage_data, size_unit = scale_storage_series_points(storage_data)
        x_domain, y_domain = extract_xy_domains_from_series_points(storage_data)

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
