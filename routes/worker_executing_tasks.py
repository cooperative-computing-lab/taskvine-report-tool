from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_time_formatter,
    d3_int_formatter,
    downsample_points_array
)
from flask import Blueprint, jsonify
from collections import defaultdict
import pandas as pd
from io import StringIO
from flask import make_response

worker_executing_tasks_bp = Blueprint('worker_executing_tasks', __name__, url_prefix='/api')

def get_worker_executing_task_points():
    base_time = runtime_state.MIN_TIME
    workers = runtime_state.workers
    tasks = runtime_state.tasks

    all_worker_events = defaultdict(list)

    for task in tasks.values():
        if not task.worker_id or not task.time_worker_start or not task.time_worker_end:
            continue
        worker = (task.worker_ip, task.worker_port)
        start = round(task.time_worker_start - base_time, 2)
        end = round(task.time_worker_end - base_time, 2)
        all_worker_events[worker].append((start, 1))
        all_worker_events[worker].append((end, -1))

    worker_keys = []
    raw_points_array = []

    for worker, events in all_worker_events.items():
        df = pd.DataFrame(events, columns=['time', 'event']).sort_values('time')
        df['cumulative'] = df['event'].cumsum()
        df = df.drop_duplicates('time', keep='last')
        points = df[['time', 'cumulative']].values.tolist()

        w = workers.get(worker)
        if w:
            for t0, t1 in zip(w.time_connected, w.time_disconnected):
                points.insert(0, [round(t0 - base_time, 2), 0])
                points.append([round(t1 - base_time, 2), 0])

        if not points:
            continue

        worker_keys.append(worker)
        raw_points_array.append(points)

    return worker_keys, raw_points_array

@worker_executing_tasks_bp.route('/worker-executing-tasks')
@check_and_reload_data()
def get_worker_executing_tasks():
    try:
        worker_keys, raw_points_array = get_worker_executing_task_points()

        if not raw_points_array:
            return jsonify({'error': 'No valid worker executing tasks data available'}), 404

        downsampled_array = downsample_points_array(raw_points_array, SAMPLING_POINTS)
        data = {}
        max_y = 0

        for worker, points in zip(worker_keys, downsampled_array):
            wid = f"{worker[0]}:{worker[1]}"
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

        merged_df = None
        for worker, points in zip(worker_keys, raw_points_array):
            wid = f"{worker[0]}:{worker[1]}"
            df = pd.DataFrame(points, columns=["time", wid])
            if merged_df is None:
                merged_df = df
            else:
                merged_df = pd.merge(merged_df, df, on="time", how="outer")

        merged_df = merged_df.sort_values("time").fillna(0).round(2)
        merged_df.columns = ["time (s)"] + [f"{col}" for col in merged_df.columns[1:]]

        buffer = StringIO()
        merged_df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=worker_executing_tasks.csv'
        response.headers['Content-Type'] = 'text/csv'
        return response

    except Exception as e:
        runtime_state.log_error(f"Error in export_worker_executing_tasks_csv: {e}")
        return jsonify({'error': str(e)}), 500