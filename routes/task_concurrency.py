from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_time_formatter,
    d3_int_formatter,
    downsample_points_array
)

import pandas as pd
from flask import Blueprint, jsonify, request, make_response
from io import StringIO

task_concurrency_bp = Blueprint(
    'task_concurrency', __name__, url_prefix='/api')


PHASE_COLUMN_TITLES = {
    'tasks_waiting': 'Waiting',
    'tasks_committing': 'Committing',
    'tasks_executing': 'Executing',
    'tasks_retrieving': 'Retrieving',
    'tasks_done': 'Done'
}

def compute_task_concurrency_points():
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

    return dict(zip(phase_keys, raw_points_array))


@task_concurrency_bp.route('/task-concurrency')
@check_and_reload_data()
def get_task_concurrency():
    try:
        raw_points_dict = compute_task_concurrency_points()

        downsampled_dict = {}
        max_y = 0
        for phase, points in raw_points_dict.items():
            downsampled = downsample_points_array([points], SAMPLING_POINTS)[0]
            downsampled_dict[phase] = downsampled
            if downsampled:
                max_y = max(max_y, max(p[1] for p in downsampled))

        data = {
            **downsampled_dict,
            'x_domain': [0, runtime_state.MAX_TIME - runtime_state.MIN_TIME],
            'y_domain': [0, max_y],
            'x_tick_values': compute_linear_tick_values([0, runtime_state.MAX_TIME - runtime_state.MIN_TIME]),
            'y_tick_values': compute_linear_tick_values([0, max_y]),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        }

        return jsonify(data)

    except Exception as e:
        runtime_state.log_error(f"Error in get_task_concurrency: {e}")
        return jsonify({'error': str(e)}), 500

@task_concurrency_bp.route('/task-concurrency/export-csv')
@check_and_reload_data()
def export_task_concurrency_csv():
    try:
        phase_points = compute_task_concurrency_points()

        if not phase_points or all(not points for points in phase_points.values()):
            return jsonify({'error': 'No task concurrency data available'}), 404

        merged_df = None
        for phase_key, points in phase_points.items():
            if not points:
                continue
            column_title = PHASE_COLUMN_TITLES.get(phase_key, phase_key)
            df = pd.DataFrame(points, columns=["time", column_title])
            if merged_df is None:
                merged_df = df
            else:
                merged_df = pd.merge(merged_df, df, on="time", how="outer")

        if merged_df is None or merged_df.empty:
            return jsonify({'error': 'No concurrency data available'}), 404

        merged_df = merged_df.sort_values("time").fillna(0)
        merged_df["time"] = merged_df["time"].round(2)

        buffer = StringIO()
        merged_df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=task_concurrency.csv"
        response.headers["Content-Type"] = "text/csv"
        return response

    except Exception as e:
        runtime_state.log_error(f"Error in export_task_concurrency_csv: {e}")
        return jsonify({'error': str(e)}), 500
