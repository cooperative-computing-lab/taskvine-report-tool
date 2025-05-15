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
from flask import Blueprint, jsonify, request

worker_storage_consumption_bp = Blueprint(
    'worker_storage_consumption', __name__, url_prefix='/api'
)

@worker_storage_consumption_bp.route('/worker-storage-consumption')
@check_and_reload_data()
def get_worker_storage_consumption():
    try:
        show_percentage = request.args.get('show_percentage', 'false').lower() == 'true'
        base_time = runtime_state.MIN_TIME
        files = runtime_state.files
        workers = runtime_state.workers

        raw_points_array = []
        worker_keys = []

        # step 1: gather transfer events per worker
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

        # step 2: build time-series for each worker
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

            # insert connection intervals
            for t0, t1 in zip(workers[worker].time_connected, workers[worker].time_disconnected):
                points.insert(0, [float(t0 - base_time), 0.0])
                points.append([float(t1 - base_time), 0.0])

            if not points or any(len(p) != 2 or np.isnan(p[0]) or np.isnan(p[1]) for p in points):
                continue

            worker_keys.append(worker)
            raw_points_array.append(points)

        # step 3: downsample
        downsampled_array = downsample_points_array(raw_points_array, SAMPLING_POINTS)

        # step 4: post-process
        storage_data = {}
        worker_resources = {}
        max_storage = 0

        for worker, points in zip(worker_keys, downsampled_array):
            wid = f"{worker[0]}:{worker[1]}"
            storage_data[wid] = points
            max_storage = max(max_storage, max(p[1] for p in points))
            w = workers[worker]
            worker_resources[wid] = {
                'cores': w.cores,
                'memory_mb': w.memory_mb,
                'disk_mb': w.disk_mb,
                'gpus': w.gpus
            }

        if not storage_data:
            return jsonify({'error': 'No valid storage consumption data available'}), 404

        # step 5: format output
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

        x_domain = [0, float(runtime_state.MAX_TIME - base_time)]
        y_domain = [0, max(1.0, max_storage)]

        # write into a file
        import json
        with open('worker_storage_consumption.json', 'w') as f:
            json.dump({
                'storage_data': storage_data,
                'worker_resources': worker_resources,
                'x_domain': x_domain,
                'y_domain': y_domain,
                'x_tick_values': compute_linear_tick_values(x_domain),
                'y_tick_values': compute_linear_tick_values(y_domain),
                'x_tick_formatter': d3_time_formatter(),
                'y_tick_formatter': y_tick_formatter,
            }, f)

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
