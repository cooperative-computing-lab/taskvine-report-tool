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

worker_waiting_retrieval_tasks_bp = Blueprint('worker_waiting_retrieval_tasks', __name__, url_prefix='/api')

@worker_waiting_retrieval_tasks_bp.route('/worker-waiting-retrieval-tasks')
@check_and_reload_data()
def get_worker_waiting_retrieval_tasks():
    try:
        base_time = runtime_state.MIN_TIME
        workers = runtime_state.workers
        tasks = runtime_state.tasks

        all_worker_events = defaultdict(list)
        for task in tasks.values():
            if not task.worker_id or not task.when_waiting_retrieval or not task.when_retrieved:
                continue
            worker = (task.worker_ip, task.worker_port)
            start = float(task.when_waiting_retrieval - base_time)
            end = float(task.when_retrieved - base_time)
            all_worker_events[worker].append((start, 1))
            all_worker_events[worker].append((end, -1))

        raw_points_array = []
        worker_keys = []
        max_waiting_retrieval_tasks = 0

        for worker, events in all_worker_events.items():
            df = pd.DataFrame(events, columns=['time', 'event']).sort_values('time')
            df['cumulative'] = df['event'].cumsum()
            df = df.drop_duplicates('time', keep='last')
            points = df[['time', 'cumulative']].values.tolist()

            # insert connection intervals for completeness
            w = workers.get(worker)
            if w:
                for t0, t1 in zip(w.time_connected, w.time_disconnected):
                    points.insert(0, [float(t0 - base_time), 0])
                    points.append([float(t1 - base_time), 0])

            if not points:
                continue
            worker_keys.append(worker)
            raw_points_array.append(points)
            max_waiting_retrieval_tasks = max(max_waiting_retrieval_tasks, max(p[1] for p in points))

        downsampled_array = downsample_points_array(raw_points_array, SAMPLING_POINTS)

        waiting_retrieval_tasks_data = {}
        for worker, points in zip(worker_keys, downsampled_array):
            wid = f"{worker[0]}:{worker[1]}"
            waiting_retrieval_tasks_data[wid] = points

        if not waiting_retrieval_tasks_data:
            return jsonify({'error': 'No valid worker waiting retrieval tasks data available'}), 404

        x_domain = [0, float(runtime_state.MAX_TIME - base_time)]
        y_domain = [0, max(1.0, max_waiting_retrieval_tasks)]

        return jsonify({
            'waiting_retrieval_tasks_data': waiting_retrieval_tasks_data,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        })
    except Exception as e:
        runtime_state.log_error(f"Error in get_worker_waiting_retrieval_tasks: {e}")
        return jsonify({'error': str(e)}), 500 