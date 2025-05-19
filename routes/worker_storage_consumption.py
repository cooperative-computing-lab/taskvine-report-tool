from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_time_formatter,
    d3_size_formatter,
    d3_percentage_formatter,
    downsample_points_array,
    get_unit_and_scale_by_max_file_size_mb
)

import pandas as pd
import numpy as np
from collections import defaultdict
from flask import Blueprint, jsonify, request, make_response
from io import StringIO

worker_storage_consumption_bp = Blueprint(
    'worker_storage_consumption', __name__, url_prefix='/api'
)

def get_worker_storage_points(show_percentage=False):
    from collections import defaultdict
    import numpy as np
    import pandas as pd

    base_time = runtime_state.MIN_TIME
    files = runtime_state.files
    workers = runtime_state.workers

    raw_points_array = []
    worker_keys = []

    all_worker_storage = defaultdict(list)
    for file in files.values():
        for transfer in file.transfers:
            dest = transfer.destination
            if not isinstance(dest, tuple) or transfer.time_stage_in is None:
                continue
            time_in = float(transfer.time_start_stage_in - base_time)
            time_out = float(transfer.time_stage_out - base_time)
            size = max(0, file.size_mb)
            all_worker_storage[dest].append((time_in, size))
            all_worker_storage[dest].append((time_out, -size))

    for worker, events in all_worker_storage.items():
        df = pd.DataFrame(events, columns=['time', 'size']).sort_values('time')
        df['storage'] = df['size'].cumsum().clip(lower=0)

        if show_percentage:
            disk = workers[worker].disk_mb
            if disk > 0:
                df['storage'] = df['storage'] / disk * 100

        df = df.groupby('time')['storage'].max().reset_index()
        if df.empty or df['storage'].isna().all():
            continue

        points = df[['time', 'storage']].values.tolist()
        for t0, t1 in zip(workers[worker].time_connected, workers[worker].time_disconnected):
            points.insert(0, [float(t0 - base_time), 0.0])
            points.append([float(t1 - base_time), 0.0])

        if not points or any(len(p) != 2 or np.isnan(p[0]) or np.isnan(p[1]) for p in points):
            continue

        worker_keys.append(worker)
        raw_points_array.append(points)

    return worker_keys, raw_points_array

@worker_storage_consumption_bp.route('/worker-storage-consumption')
@check_and_reload_data()
def get_worker_storage_consumption():
    try:
        show_percentage = request.args.get('show_percentage', 'false').lower() == 'true'
        worker_keys, raw_points_array = get_worker_storage_points(show_percentage)

        if not raw_points_array:
            return jsonify({'error': 'No valid storage consumption data available'}), 404

        downsampled_array = downsample_points_array(raw_points_array, SAMPLING_POINTS)

        storage_data = {}
        worker_resources = {}
        max_storage = 0

        for worker, points in zip(worker_keys, downsampled_array):
            wid = f"{worker[0]}:{worker[1]}"
            storage_data[wid] = points
            max_storage = max(max_storage, max(p[1] for p in points))
            w = runtime_state.workers[worker]
            worker_resources[wid] = {
                'cores': w.cores,
                'memory_mb': w.memory_mb,
                'disk_mb': w.disk_mb,
                'gpus': w.gpus
            }

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

        x_domain = [0, float(runtime_state.MAX_TIME - runtime_state.MIN_TIME)]
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
        runtime_state.log_error(f"Error in get_worker_storage_consumption: {e}")
        return jsonify({'error': str(e)}), 500

@worker_storage_consumption_bp.route('/worker-storage-consumption/export-csv')
@check_and_reload_data()
def export_worker_storage_consumption_csv():
    try:
        show_percentage = request.args.get('show_percentage', 'false').lower() == 'true'
        worker_keys, raw_points_array = get_worker_storage_points(show_percentage)

        if not raw_points_array:
            return jsonify({'error': 'No valid storage consumption data available'}), 404

        df_list = []
        for worker, points in zip(worker_keys, raw_points_array):
            wid = f"{worker[0]}:{worker[1]}"
            df = pd.DataFrame(points, columns=['time', wid]).set_index('time')
            df_list.append(df)

        merged_df = pd.concat(df_list, axis=1).fillna(0).reset_index()

        if show_percentage:
            unit_label = "(%)"
        else:
            max_val = merged_df.drop(columns="time").to_numpy().max()
            unit_label = f"({get_unit_and_scale_by_max_file_size_mb(max_val)[0]})"

        merged_df.columns = ["time (s)"] + [f"{col} {unit_label}" for col in merged_df.columns[1:]]

        buffer = StringIO()
        merged_df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=worker_storage_consumption.csv'
        response.headers['Content-Type'] = 'text/csv'
        return response

    except Exception as e:
        runtime_state.log_error(f"Error in export_worker_storage_consumption_csv: {e}")
        return jsonify({'error': str(e)}), 500
