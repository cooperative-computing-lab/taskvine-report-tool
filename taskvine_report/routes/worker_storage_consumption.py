from .utils import *

import pandas as pd
from flask import Blueprint, jsonify, request, Response, current_app
from collections import defaultdict

worker_storage_consumption_bp = Blueprint(
    'worker_storage_consumption', __name__, url_prefix='/api'
)

def get_worker_storage_points(show_percentage=False):
    base_time = current_app.config["RUNTIME_STATE"].MIN_TIME
    files = current_app.config["RUNTIME_STATE"].files
    workers = current_app.config["RUNTIME_STATE"].workers

    all_worker_storage = defaultdict(list)
    for file in files.values():
        for transfer in file.transfers:
            dest = transfer.destination
            if not isinstance(dest, tuple) or transfer.time_stage_in is None:
                continue
            time_in = floor_decimal(float(transfer.time_start_stage_in - base_time), 2)
            time_out = floor_decimal(float(transfer.time_stage_out - base_time), 2)
            size = max(0, file.size_mb)
            all_worker_storage[dest].append((time_in, size))
            all_worker_storage[dest].append((time_out, -size))

    raw_points_array = []
    worker_keys = []

    for worker_entry, events in all_worker_storage.items():
        if not events:
            continue

        df = pd.DataFrame(events, columns=['time', 'size']).sort_values('time')
        df['storage'] = df['size'].cumsum().clip(lower=0)

        if show_percentage:
            disk = workers[worker_entry].disk_mb
            if disk > 0:
                df['storage'] = df['storage'] / disk * 100

        df['time'] = df['time'].map(lambda x: floor_decimal(x, 2))
        df = df.groupby('time', as_index=False)['storage'].agg(prefer_zero_else_max)

        # the base_time might be the time when the first task is dispatched, but workers might be
        # connected before that, so we need to skip those points
        time_boundary_points = None
        if workers[worker_entry]:
            time_boundary_points = get_worker_time_boundary_points(workers[worker_entry], base_time)

        if time_boundary_points:
            boundary_df = pd.DataFrame(time_boundary_points, columns=['time', 'storage'])
            full_df = pd.concat([df, boundary_df], ignore_index=True).sort_values('time')
        else:
            full_df = df

        # we need to use .max() for most of the points to reflect the maximum storage consumption
        # however, we also need to ensure that the first point is 0 and the last point is 0
        full_df = full_df.groupby('time', as_index=False)['storage'].agg(prefer_zero_else_max).sort_values('time')

        if full_df['storage'].isna().all():
            continue

        worker_keys.append(worker_entry)
        compressed_points = compress_time_based_critical_points(full_df.values.tolist())
        raw_points_array.append(compressed_points)

    return worker_keys, raw_points_array


@worker_storage_consumption_bp.route('/worker-storage-consumption')
@check_and_reload_data()
def get_worker_storage_consumption():
    try:
        show_percentage = request.args.get('show_percentage', 'false').lower() == 'true'
        worker_keys, raw_points_array = get_worker_storage_points(show_percentage)

        if not raw_points_array:
            return jsonify({'error': 'No valid storage consumption data available'}), 404

        storage_data = {}
        worker_resources = {}
        max_storage = 0

        for worker, points in zip(worker_keys, raw_points_array):
            wid = f"{worker[0]}:{worker[1]}"
            storage_data[wid] = points
            max_storage = max(max_storage, max(p[1] for p in points))
            w = current_app.config["RUNTIME_STATE"].workers[worker]
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


@worker_storage_consumption_bp.route('/worker-storage-consumption/export-csv')
@check_and_reload_data()
def export_worker_storage_consumption_csv():
    try:
        show_percentage = request.args.get('show_percentage', 'false').lower() == 'true'
        worker_keys, raw_points_array = get_worker_storage_points(show_percentage)

        if not raw_points_array:
            return jsonify({'error': 'No valid storage consumption data available'}), 404

        time_set = set()
        column_data = {}
        for worker, points in zip(worker_keys, raw_points_array):
            wid = f"{worker[0]}:{worker[1]}"
            col_map = {floor_decimal(t, 2): v for t, v in points}
            column_data[wid] = col_map
            time_set.update(col_map.keys())

        sorted_times = sorted(time_set)
        column_names = list(column_data.keys())

        if show_percentage:
            unit_label = "(%)"
        else:
            max_val = max((max(col.values(), default=0) for col in column_data.values()), default=0)
            unit_label = f"({get_unit_and_scale_by_max_file_size_mb(max_val)[0]})"

        header = ",".join(["Time (s)"] + [f"{name} {unit_label}" for name in column_names])
        get_row = lambda t: ",".join(
            [f"{t:.2f}"] + [f"{column_data[name].get(t, 0):.6f}" for name in column_names]
        )

        def generate_csv():
            yield header + "\n"
            yield from (get_row(t) + "\n" for t in sorted_times)

        return Response(generate_csv(),
                        headers={
                            "Content-Disposition": "attachment; filename=worker_storage_consumption.csv",
                            "Content-Type": "text/csv"
                        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_worker_storage_consumption_csv: {e}")
        return jsonify({'error': str(e)}), 500
