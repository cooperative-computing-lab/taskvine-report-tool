from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_time_formatter,
    d3_int_formatter,
    downsample_points_array
)

import pandas as pd
from flask import Blueprint, jsonify, request

task_concurrency_bp = Blueprint(
    'task_concurrency', __name__, url_prefix='/api')


@task_concurrency_bp.route('/task-concurrency')
@check_and_reload_data()
def get_task_concurrency():
    try:
        task_phases = {
            'tasks_waiting': [],
            'tasks_committing': [],
            'tasks_executing': [],
            'tasks_retrieving': [],
            'tasks_done': [],
        }

        for task in runtime_state.tasks.values():
            if task.when_ready:
                task_phases['tasks_waiting'].append((max(task.when_ready - runtime_state.MIN_TIME, 0), 1))
                if task.when_running:
                    task_phases['tasks_waiting'].append((task.when_running - runtime_state.MIN_TIME, -1))

            if task.when_running:
                task_phases['tasks_committing'].append((task.when_running - runtime_state.MIN_TIME, 1))
                if task.time_worker_start:
                    task_phases['tasks_committing'].append((task.time_worker_start - runtime_state.MIN_TIME, -1))

            if task.time_worker_start:
                task_phases['tasks_executing'].append((task.time_worker_start - runtime_state.MIN_TIME, 1))
                if task.time_worker_end:
                    task_phases['tasks_executing'].append((task.time_worker_end - runtime_state.MIN_TIME, -1))

            if task.time_worker_end:
                task_phases['tasks_retrieving'].append((task.time_worker_end - runtime_state.MIN_TIME, 1))
                if task.when_retrieved:
                    task_phases['tasks_retrieving'].append((task.when_retrieved - runtime_state.MIN_TIME, -1))

            if task.when_done:
                task_phases['tasks_done'].append((task.when_done - runtime_state.MIN_TIME, 1))

        raw_points_array = []
        phase_keys = list(task_phases.keys())

        for events in task_phases.values():
            if not events:
                raw_points_array.append([])
                continue
            df = pd.DataFrame(events, columns=['time', 'event']).sort_values('time')
            df['time'] = df['time'].round(2)
            df['cumulative'] = df['event'].cumsum()
            df = df.drop_duplicates('time', keep='last')
            raw_points_array.append(df[['time', 'cumulative']].values.tolist())

        downsampled_array = downsample_points_array(raw_points_array, SAMPLING_POINTS)
        data = {}
        max_y = 0
        for key, points in zip(phase_keys, downsampled_array):
            data[key] = points
            if points:
                max_y = max(max_y, max(p[1] for p in points))

        data.update({
            'x_domain': [0, runtime_state.MAX_TIME - runtime_state.MIN_TIME],
            'y_domain': [0, max_y],
            'x_tick_values': compute_linear_tick_values([0, runtime_state.MAX_TIME - runtime_state.MIN_TIME]),
            'y_tick_values': compute_linear_tick_values([0, max_y]),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        })

        return jsonify(data)

    except Exception as e:
        print(f"Error in get_task_concurrency: {str(e)}")
        return jsonify({'error': str(e)}), 500