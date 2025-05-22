from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_time_formatter,
    d3_int_formatter,
    floor_decimal,
    compress_time_based_critical_points
)
from flask import Blueprint, jsonify, Response
from collections import defaultdict
import pandas as pd

worker_executing_tasks_bp = Blueprint('worker_executing_tasks', __name__, url_prefix='/api')

def get_worker_executing_task_points():
    base_time = runtime_state.MIN_TIME
    workers = runtime_state.workers
    tasks = runtime_state.tasks

    raw_points_array = []
    worker_keys = []

    all_worker_events = defaultdict(list)

    for task in tasks.values():
        if not task.worker_entry or not task.time_worker_start or not task.time_worker_end:
            continue
        worker_entry = task.worker_entry
        start = floor_decimal(task.time_worker_start - base_time, 2)
        end = floor_decimal(task.time_worker_end - base_time, 2)
        if start >= end:
            continue
        all_worker_events[worker_entry].extend([(start, 1), (end, -1)])

    for worker_entry, events in all_worker_events.items():
        
        w = workers.get(worker_entry)
        if w:
            boundary_times = [floor_decimal(t - base_time, 2) for t in w.time_connected + w.time_disconnected]
            events += [(t, 0) for t in boundary_times]

        if not events:
            continue

        df = pd.DataFrame(events, columns=['time', 'delta'])
        df = df.groupby('time', as_index=False)['delta'].sum()
        df['cumulative'] = df['delta'].cumsum()
        df['cumulative'] = df['cumulative'].clip(lower=0)

        compressed_points = compress_time_based_critical_points(df[['time', 'cumulative']].values.tolist())
        raw_points_array.append(compressed_points)
        worker_keys.append(worker_entry)

    return worker_keys, raw_points_array


@worker_executing_tasks_bp.route('/worker-executing-tasks')
@check_and_reload_data()
def get_worker_executing_tasks():
    try:
        worker_keys, raw_points_array = get_worker_executing_task_points()

        if not raw_points_array:
            return jsonify({'error': 'No valid worker executing tasks data available'}), 404

        data = {}
        max_y = 0

        for worker_entry, points in zip(worker_keys, raw_points_array):
            wid = f"{worker_entry[0]}:{worker_entry[1]}:{worker_entry[2]}"
            data[wid] = points
            max_y = max(max_y, max(p[1] for p in points))

        x_domain = [0, float(runtime_state.MAX_TIME - runtime_state.MIN_TIME)]
        y_domain = [0, max(1.0, max_y)]

        return jsonify({
            'executing_tasks_data': data,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        })
    except Exception as e:
        runtime_state.log_error(f"Error in get_worker_executing_tasks: {e}")
        return jsonify({'error': str(e)}), 500

@worker_executing_tasks_bp.route('/worker-executing-tasks/export-csv')
@check_and_reload_data()
def export_worker_executing_tasks_csv():
    try:
        worker_keys, raw_points_array = get_worker_executing_task_points()
        if not raw_points_array:
            return jsonify({'error': 'No valid worker executing tasks data available'}), 404

        column_data = {}
        time_set = set()

        for worker_entry, points in zip(worker_keys, raw_points_array):
            wid = f"{worker_entry[0]}:{worker_entry[1]}:{worker_entry[2]}"
            col_map = {floor_decimal(t, 2): v for t, v in points}
            column_data[wid] = col_map
            time_set.update(col_map.keys())

        sorted_times = sorted(time_set)
        columns = list(column_data.keys())

        def generate_csv():
            yield "time (s)," + ",".join(columns) + "\n"
            for t in sorted_times:
                row = [f"{t:.2f}"] + [f"{column_data[c].get(t, 0):.6f}" for c in columns]
                yield ",".join(row) + "\n"

        return Response(
            generate_csv(),
            headers={
                "Content-Disposition": "attachment; filename=worker_executing_tasks.csv",
                "Content-Type": "text/csv"
            }
        )
    except Exception as e:
        runtime_state.log_error(f"Error in export_worker_executing_tasks_csv: {e}")
        return jsonify({'error': str(e)}), 500