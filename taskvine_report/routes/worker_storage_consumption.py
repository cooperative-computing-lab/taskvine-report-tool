from .utils import *
import pandas as pd
import os
from flask import Blueprint, jsonify, request, Response, current_app

worker_storage_consumption_bp = Blueprint(
    'worker_storage_consumption', __name__, url_prefix='/api'
)

@worker_storage_consumption_bp.route('/worker-storage-consumption')
@check_and_reload_data()
def get_worker_storage_consumption():
    try:
        show_percentage = request.args.get('show_percentage', 'false').lower() == 'true'
        
        # Select the appropriate CSV file
        csv_path = (current_app.config["RUNTIME_STATE"].csv_file_worker_storage_consumption_percentage 
                   if show_percentage 
                   else current_app.config["RUNTIME_STATE"].csv_file_worker_storage_consumption)
        
        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404
        
        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({'error': 'CSV is empty'}), 404

        time_column = 'time (s)'
        worker_columns = [col for col in df.columns if col != time_column]
        
        storage_data = {}
        worker_resources = {}
        max_storage = 0

        for worker_col in worker_columns:
            # Use dropna to preserve missing data semantics
            points = df[[time_column, worker_col]].dropna().values.tolist()
            if points:
                # Parse worker key back to tuple format for resource lookup
                parts = worker_col.split(':')
                if len(parts) == 3:
                    worker_entry = (parts[0], int(parts[1]), int(parts[2]))
                    # Use simplified worker ID (ip:port) for frontend
                    wid = f"{parts[0]}:{parts[1]}"
                    storage_data[wid] = points
                    max_storage = max(max_storage, max(p[1] for p in points))
                    
                    # Get worker resources if available
                    if worker_entry in current_app.config["RUNTIME_STATE"].workers:
                        w = current_app.config["RUNTIME_STATE"].workers[worker_entry]
                        worker_resources[wid] = {
                            'cores': w.cores,
                            'memory_mb': w.memory_mb,
                            'disk_mb': w.disk_mb,
                            'gpus': w.gpus
                        }

        if not storage_data:
            return jsonify({'error': 'No valid storage consumption data available'}), 404

        # Set up formatting based on percentage or absolute values
        if show_percentage:
            max_storage = 100
            y_tick_formatter = d3_percentage_formatter()
        else:
            unit, scale = get_unit_and_scale_by_max_file_size_mb(max_storage)
            if scale != 1:
                for wid in storage_data:
                    storage_data[wid] = [[x, y * scale] for x, y in storage_data[wid]]
                max_storage *= scale
            y_tick_formatter = d3_size_formatter(unit)

        x_domain = [0, float(current_app.config["RUNTIME_STATE"].MAX_TIME - current_app.config["RUNTIME_STATE"].MIN_TIME)]
        y_domain = [0, max(1.0, max_storage)]

        return jsonify({
            'storage_data': storage_data,
            'worker_resources': worker_resources,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': y_tick_formatter,
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_worker_storage_consumption: {e}")
        return jsonify({'error': str(e)}), 500
