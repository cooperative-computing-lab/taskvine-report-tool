from .utils import *
from flask import Blueprint, jsonify, Response, current_app
from collections import defaultdict
import pandas as pd 

worker_waiting_retrieval_tasks_bp = Blueprint('worker_waiting_retrieval_tasks', __name__, url_prefix='/api')

def get_worker_waiting_retrieval_task_points():
    base_time = current_app.config["RUNTIME_STATE"].MIN_TIME
    tasks = current_app.config["RUNTIME_STATE"].tasks
    workers = current_app.config["RUNTIME_STATE"].workers

    raw_points_array = []
    worker_keys = []

    all_worker_events = defaultdict(list)

    for task in tasks.values():
        if not task.worker_entry or not task.when_waiting_retrieval or not task.when_retrieved:
            continue
        worker_entry = task.worker_entry
        start = floor_decimal(task.when_waiting_retrieval - base_time, 2)
        end = floor_decimal(task.when_retrieved - base_time, 2)
        if start >= end:
            continue
        all_worker_events[worker_entry].extend([(start, 1), (end, -1)])

    for worker_entry, events in all_worker_events.items():
        w = workers.get(worker_entry)
        time_boundary_points = None
        if w:
            time_boundary_points = get_worker_time_boundary_points(w, base_time)
        all_events = events + time_boundary_points

        df = pd.DataFrame(all_events, columns=['time', 'delta'])
        df = df.groupby('time', as_index=False)['delta'].sum()
        df['cumulative'] = df['delta'].cumsum()
        df['cumulative'] = df['cumulative'].clip(lower=0)

        if df['cumulative'].isna().all():
            continue

        compressed_points = compress_time_based_critical_points(df[['time', 'cumulative']].values.tolist())
        raw_points_array.append(compressed_points)
        worker_keys.append(worker_entry)

    return worker_keys, raw_points_array


@worker_waiting_retrieval_tasks_bp.route('/worker-waiting-retrieval-tasks')
@check_and_reload_data()
def get_worker_waiting_retrieval_tasks():
    try:
        worker_keys, raw_points_array = get_worker_waiting_retrieval_task_points()

        if not raw_points_array:
            return jsonify({'error': 'No valid worker waiting retrieval tasks data available'}), 404

        data = {}
        max_y = 0
        for worker_entry, points in zip(worker_keys, raw_points_array):
            wid = f"{worker_entry[0]}:{worker_entry[1]}:{worker_entry[2]}"
            data[wid] = points
            max_y = max(max_y, max(p[1] for p in points))

        x_domain = [0, float(current_app.config["RUNTIME_STATE"].MAX_TIME - current_app.config["RUNTIME_STATE"].MIN_TIME)]
        y_domain = [0, max(1.0, max_y)]

        return jsonify({
            'waiting_retrieval_tasks_data': data,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_worker_waiting_retrieval_tasks: {e}")
        return jsonify({'error': str(e)}), 500

@worker_waiting_retrieval_tasks_bp.route('/worker-waiting-retrieval-tasks/export-csv')
@check_and_reload_data()
def export_worker_waiting_retrieval_tasks_csv():
    try:
        worker_keys, raw_points_array = get_worker_waiting_retrieval_task_points()
        if not raw_points_array:
            return jsonify({'error': 'No valid worker waiting retrieval tasks data available'}), 404

        column_data = {}
        time_set = set()

        for worker_entry, points in zip(worker_keys, raw_points_array):
            wid = f"{worker_entry[0]}:{worker_entry[1]}:{worker_entry[2]}"
            col_map = {}

            for i, row in enumerate(points):
                try:
                    if isinstance(row, (list, tuple)) and len(row) >= 2:
                        t = floor_decimal(float(row[0]), 2)
                        v = floor_decimal(float(row[1]), 2)
                        col_map[t] = v
                        time_set.add(t)
                    else:
                        current_app.config["RUNTIME_STATE"].log_error(f"Skipping malformed row at index {i} for {wid}: {row}")
                except Exception as e:
                    current_app.config["RUNTIME_STATE"].log_error(f"Error parsing row {row} for {wid}: {e}")
                    continue

            column_data[wid] = col_map

        if not time_set:
            return jsonify({'error': 'No valid time points found'}), 404

        sorted_times = sorted(time_set)
        columns = list(column_data.keys())

        def generate_csv():
            yield "time (s)," + ",".join(columns) + "\n"
            for t in sorted_times:
                row = [f"{t:.2f}"]
                for c in columns:
                    val = column_data[c].get(t, 0)
                    row.append(f"{val:.2f}")
                yield ",".join(row) + "\n"

        return Response(
            generate_csv(),
            headers={
                "Content-Disposition": "attachment; filename=worker_waiting_retrieval_tasks.csv",
                "Content-Type": "text/csv"
            }
        )

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_worker_waiting_retrieval_tasks_csv: {e}")
        return jsonify({'error': str(e)}), 500
