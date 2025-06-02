from .utils import *

import pandas as pd
from flask import Blueprint, jsonify, make_response, current_app
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

    base_time = current_app.config["RUNTIME_STATE"].MIN_TIME

    for task in current_app.config["RUNTIME_STATE"].tasks.values():
        if task.when_ready:
            # ready tasks can happen before the base time
            t0 = floor_decimal(max(task.when_ready - base_time, 0), 2)
            task_phases['tasks_waiting'].append((t0, 1))
            time_waiting_end = None
            if task.when_running:
                time_waiting_end = task.when_running
            elif task.when_failure_happens:
                time_waiting_end = task.when_failure_happens
            if time_waiting_end:
                t1 = floor_decimal(time_waiting_end - base_time, 2)
                task_phases['tasks_waiting'].append((t1, -1))

        if task.when_running:
            t0 = floor_decimal(task.when_running - base_time, 2)
            task_phases['tasks_committing'].append((t0, 1))
            time_committing_end = None
            if task.time_worker_start:
                time_committing_end = task.time_worker_start
            elif task.when_failure_happens:
                time_committing_end = task.when_failure_happens
            elif task.when_waiting_retrieval:
                time_committing_end = task.when_waiting_retrieval
            else:
                print(f"Task {task.task_id} has no time_worker_start or when_failure_happens")
            if time_committing_end:    
                t1 = floor_decimal(time_committing_end - base_time, 2)
                task_phases['tasks_committing'].append((t1, -1))
            else:
                print(f"Task {task.task_id} has no time_committing_end")

        if task.time_worker_start:
            t0 = floor_decimal(task.time_worker_start - base_time, 2)
            if t0 < 0:
                current_app.config["RUNTIME_STATE"].log_error(f"Task {task.task_id} has negative time_worker_start: {task.time_worker_start} - {base_time} = {t0}")
                task.print_info()
            task_phases['tasks_executing'].append((t0, 1))
            time_executing_end = None
            if task.time_worker_end:
                time_executing_end = task.time_worker_end
            elif task.when_failure_happens:
                time_executing_end = task.when_failure_happens
            elif task.when_waiting_retrieval:
                time_executing_end = task.when_waiting_retrieval
            if time_executing_end:
                t1 = floor_decimal(time_executing_end - base_time, 2)
                task_phases['tasks_executing'].append((t1, -1))

        if task.time_worker_end:
            t0 = floor_decimal(task.time_worker_end - base_time, 2)
            task_phases['tasks_retrieving'].append((t0, 1))
            time_retrieving_end = None
            if task.when_waiting_retrieval:
                time_retrieving_end = task.when_waiting_retrieval
            elif task.when_failure_happens:
                time_retrieving_end = task.when_failure_happens
            if time_retrieving_end:
                t1 = floor_decimal(time_retrieving_end - base_time, 2)
                task_phases['tasks_retrieving'].append((t1, -1))

        if task.when_done:
            t0 = floor_decimal(task.when_done - base_time, 2)
            task_phases['tasks_done'].append((t0, 1))

    raw_points_array = []
    phase_keys = list(task_phases.keys())

    for events in task_phases.values():
        if not events:
            raw_points_array.append([])
            continue
        df = pd.DataFrame(events, columns=['time', 'event']).sort_values('time')
        df = df.groupby('time')['event'].sum().reset_index()
        df['cumulative'] = df['event'].cumsum().clip(lower=0)
        raw_points_array.append(compress_time_based_critical_points(df[['time', 'cumulative']].values.tolist()))

    return dict(zip(phase_keys, raw_points_array))

@task_concurrency_bp.route('/task-concurrency')
@check_and_reload_data()
def get_task_concurrency():
    try:
        raw_points_dict = compute_task_concurrency_points()

        downsampled_dict = {}
        max_y = 0
        for phase, points in raw_points_dict.items():
            downsampled_dict[phase] = points
            if points:
                max_y = max(max_y, max(p[1] for p in points))

        data = {
            **downsampled_dict,
            'x_domain': [0, current_app.config["RUNTIME_STATE"].MAX_TIME - current_app.config["RUNTIME_STATE"].MIN_TIME],
            'y_domain': [0, max_y],
            'x_tick_values': compute_linear_tick_values([0, current_app.config["RUNTIME_STATE"].MAX_TIME - current_app.config["RUNTIME_STATE"].MIN_TIME]),
            'y_tick_values': compute_linear_tick_values([0, max_y]),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        }

        return jsonify(data)

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_concurrency: {e}")
        return jsonify({'error': str(e)}), 500

@task_concurrency_bp.route('/task-concurrency/export-csv')
@check_and_reload_data()
def export_task_concurrency_csv():
    try:
        phase_points = compute_task_concurrency_points()

        if not phase_points or all(not points for points in phase_points.values()):
            return jsonify({'error': 'No task concurrency data available'}), 404

        df_list = []
        for phase_key, points in phase_points.items():
            if not points:
                continue
            column_title = PHASE_COLUMN_TITLES.get(phase_key, phase_key.capitalize())
            df = pd.DataFrame(points, columns=["time", column_title])
            df = df.groupby("time")[column_title].agg(prefer_zero_else_max).reset_index()
            df = df.set_index("time")
            df_list.append(df)

        if not df_list:
            return jsonify({'error': 'No concurrency data available'}), 404

        merged_df = pd.concat(df_list, axis=1).fillna(0).reset_index()
        merged_df["time"] = merged_df["time"].map(lambda x: floor_decimal(x, 2))
        
        merged_df = merged_df.sort_values("time")

        buffer = StringIO()
        merged_df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=task_concurrency.csv"
        response.headers["Content-Type"] = "text/csv"
        return response

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_task_concurrency_csv: {e}")
        return jsonify({'error': str(e)}), 500
