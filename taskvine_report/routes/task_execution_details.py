from taskvine_report.utils import *

import pandas as pd
from collections import defaultdict
from flask import Blueprint, jsonify, make_response, current_app
from io import StringIO

task_execution_details_bp = Blueprint(
    'task_execution_details', __name__, url_prefix='/api')

TASK_STATUS_TO_CHECKBOX_NAME = {
    1: 'unsuccessful-input-missing',
    2: 'unsuccessful-output-missing',
    4: 'unsuccessful-stdout-missing',
    1 << 3: 'unsuccessful-signal',
    2 << 3: 'unsuccessful-resource-exhaustion',
    3 << 3: 'unsuccessful-max-end-time',
    4 << 3: 'unsuccessful-unknown',
    5 << 3: 'unsuccessful-forsaken',
    6 << 3: 'unsuccessful-max-retries',
    7 << 3: 'unsuccessful-max-wall-time',
    8 << 3: 'unsuccessful-monitor-error',
    9 << 3: 'unsuccessful-output-transfer-error',
    10 << 3: 'unsuccessful-location-missing',
    11 << 3: 'unsuccessful-cancelled',
    12 << 3: 'unsuccessful-library-exit',
    13 << 3: 'unsuccessful-sandbox-exhaustion',
    14 << 3: 'unsuccessful-missing-library',
    15 << 3: 'unsuccessful-worker-disconnected',
}

LEGEND_SCHEMA = {
    'workers': ('Workers', 'Workers', 'lightgrey'),

    'successful-committing-to-worker': ('Successful Tasks', 'Committing', '#0ecfc8'),
    'successful-executing-on-worker':  ('Successful Tasks', 'Executing', 'steelblue'),
    'successful-retrieving-to-manager': ('Successful Tasks', 'Retrieving', '#cc5a12'),

    'recovery-successful': ('Recovery Tasks', 'Successful', '#FF69B4'),
    'recovery-unsuccessful': ('Recovery Tasks', 'Unsuccessful', '#E3314F'),

    'unsuccessful-input-missing': ('Unsuccessful Tasks', 'Input Missing', '#FFB6C1'),
    'unsuccessful-output-missing': ('Unsuccessful Tasks', 'Output Missing', '#FF69B4'),
    'unsuccessful-stdout-missing': ('Unsuccessful Tasks', 'Stdout Missing', '#FF1493'),
    'unsuccessful-signal': ('Unsuccessful Tasks', 'Signal', '#CD5C5C'),
    'unsuccessful-resource-exhaustion': ('Unsuccessful Tasks', 'Resource Exhaustion', '#8B0000'),
    'unsuccessful-max-end-time': ('Unsuccessful Tasks', 'Max End Time', '#B22222'),
    'unsuccessful-unknown': ('Unsuccessful Tasks', 'Unknown', '#A52A2A'),
    'unsuccessful-forsaken': ('Unsuccessful Tasks', 'Forsaken', '#E331EE'),
    'unsuccessful-max-retries': ('Unsuccessful Tasks', 'Max Retries', '#8B4513'),
    'unsuccessful-max-wall-time': ('Unsuccessful Tasks', 'Max Wall Time', '#D2691E'),
    'unsuccessful-monitor-error': ('Unsuccessful Tasks', 'Monitor Error', '#FF4444'),
    'unsuccessful-output-transfer-error': ('Unsuccessful Tasks', 'Output Transfer Error', '#FF6B6B'),
    'unsuccessful-location-missing': ('Unsuccessful Tasks', 'Location Missing', '#FF8787'),
    'unsuccessful-cancelled': ('Unsuccessful Tasks', 'Cancelled', '#FFA07A'),
    'unsuccessful-library-exit': ('Unsuccessful Tasks', 'Library Exit', '#FA8072'),
    'unsuccessful-sandbox-exhaustion': ('Unsuccessful Tasks', 'Sandbox Exhaustion', '#E9967A'),
    'unsuccessful-missing-library': ('Unsuccessful Tasks', 'Missing Library', '#F08080'),
    'unsuccessful-worker-disconnected': ('Unsuccessful Tasks', 'Worker Disconnected', '#FF0000'),
}

def calculate_legend(successful_tasks, unsuccessful_tasks, workers):
    counts = defaultdict(int)

    for task in successful_tasks:
        counts['successful-committing-to-worker'] += 1
        counts['successful-executing-on-worker'] += 1
        counts['successful-retrieving-to-manager'] += 1
        if task['is_recovery_task']:
            counts['recovery-successful'] += 1

    for task in unsuccessful_tasks:
        key = TASK_STATUS_TO_CHECKBOX_NAME.get(task['task_status'])
        if key:
            counts[key] += 1
            if task['is_recovery_task']:
                counts['recovery-unsuccessful'] += 1

    counts['workers'] = len(workers)

    group_map = defaultdict(lambda: {'total': 0, 'items': []})
    for key, (group, label, color) in LEGEND_SCHEMA.items():
        count = counts.get(key, 0)
        if count == 0:
            continue
        group_map[group]['items'].append({
            'id': key,
            'label': label,
            'count': count,
            'color': color,
        })

    for group in ['Successful Tasks', 'Unsuccessful Tasks']:
        group_map[group]['total'] = len(successful_tasks if group == 'Successful Tasks' else unsuccessful_tasks)

    for group in ['Recovery Tasks', 'Workers']:
        group_map[group]['total'] = sum(item['count'] for item in group_map[group]['items'])

    legend = []
    for group in ['Successful Tasks', 'Unsuccessful Tasks', 'Recovery Tasks', 'Workers']:
        legend.append({
            'group': group,
            'total': group_map[group]['total'],
            'items': group_map[group]['items']
        })

    return legend

def downsample_tasks(tasks, key="execution_time", max_tasks=None):
    if not max_tasks:
        max_tasks = current_app.config["SAMPLING_TASK_BARS"]

    if len(tasks) <= max_tasks:
        return tasks

    # sort tasks by execution time
    tasks = sorted(tasks, key=lambda x: x[key], reverse=True)

    return tasks[:max_tasks]

@task_execution_details_bp.route('/task-execution-details')
@check_and_reload_data()
def get_task_execution_details():
    try:
        successful_tasks = []
        unsuccessful_tasks = []
        workers = []

        for task in current_app.config["RUNTIME_STATE"].tasks.values():
            if not task.core_id:
                continue
            if not task.worker_entry:
                continue

            worker = current_app.config["RUNTIME_STATE"].workers[task.worker_entry]
            worker_id = worker.id

            if task.task_status == 0:
                if not task.when_retrieved or task.is_library_task:
                    continue

                successful_tasks.append({
                    'task_id': task.task_id,
                    'try_id': task.task_try_id,
                    'worker_entry': task.worker_entry,
                    'worker_id': worker_id,
                    'core_id': task.core_id[0],
                    'is_recovery_task': task.is_recovery_task,
                    'input_files': file_list_formatter(task.input_files),
                    'output_files': file_list_formatter(task.output_files),
                    'num_input_files': len(task.input_files),
                    'num_output_files': len(task.output_files),
                    'task_status': task.task_status,
                    'category': task.category,
                    'when_ready': task.when_ready - current_app.config["RUNTIME_STATE"].MIN_TIME,
                    'when_running': task.when_running - current_app.config["RUNTIME_STATE"].MIN_TIME,
                    'time_worker_start': task.time_worker_start - current_app.config["RUNTIME_STATE"].MIN_TIME,
                    'time_worker_end': task.time_worker_end - current_app.config["RUNTIME_STATE"].MIN_TIME,
                    'execution_time': task.time_worker_end - task.time_worker_start,
                    'when_waiting_retrieval': task.when_waiting_retrieval - current_app.config["RUNTIME_STATE"].MIN_TIME,
                    'when_retrieved': task.when_retrieved - current_app.config["RUNTIME_STATE"].MIN_TIME,
                    'when_done': task.when_done - current_app.config["RUNTIME_STATE"].MIN_TIME if task.when_done else 'N/A'
                })
            else:
                unsuccessful_tasks.append({
                    'task_id': task.task_id,
                    'try_id': task.task_try_id,
                    'worker_entry': task.worker_entry,
                    'worker_id': worker_id,
                    'core_id': task.core_id[0],
                    'is_recovery_task': task.is_recovery_task,
                    'input_files': file_list_formatter(task.input_files),
                    'output_files': file_list_formatter(task.output_files),
                    'num_input_files': len(task.input_files),
                    'num_output_files': len(task.output_files),
                    'task_status': task.task_status,
                    'category': task.category,
                    'when_ready': task.when_ready - current_app.config["RUNTIME_STATE"].MIN_TIME,
                    'when_running': task.when_running - current_app.config["RUNTIME_STATE"].MIN_TIME,
                    'when_failure_happens': task.when_failure_happens - current_app.config["RUNTIME_STATE"].MIN_TIME,
                    'execution_time': task.when_failure_happens - task.when_running,
                    'unsuccessful_checkbox_name': TASK_STATUS_TO_CHECKBOX_NAME.get(task.task_status, 'unknown'),
                    'when_done': task.when_done - current_app.config["RUNTIME_STATE"].MIN_TIME if task.when_done else 'N/A'
                })

        for w in current_app.config["RUNTIME_STATE"].workers.values():
            if not w.hash:
                continue

            if len(w.time_disconnected) != len(w.time_connected):
                w.time_disconnected = [current_app.config["RUNTIME_STATE"].MAX_TIME] * (len(w.time_connected) - len(w.time_disconnected))

            workers.append({
                'hash': w.hash,
                'id': w.id,
                'worker_entry': f"{w.ip}:{w.port}:{w.connect_id}",
                'time_connected': [max(t - current_app.config["RUNTIME_STATE"].MIN_TIME, 0) for t in w.time_connected],
                'time_disconnected': [max(t - current_app.config["RUNTIME_STATE"].MIN_TIME, 0) for t in w.time_disconnected],
                'cores': w.cores,
                'memory_mb': w.memory_mb,
                'disk_mb': w.disk_mb,
                'gpus': w.gpus
            })

        y_domain = [f"{w['id']}-{i}" for w in workers for i in range(1, w['cores'] + 1)]
        if len(y_domain) <= 5:
            y_tick_values = y_domain
        else:
            num_ticks = 5
            step = (len(y_domain) - 1) / (num_ticks - 1)
            indices = [round(i * step) for i in range(num_ticks)]
            y_tick_values = [y_domain[i] for i in indices]

        data = {
            'legend': calculate_legend(successful_tasks, unsuccessful_tasks, workers),
            'successful_tasks': downsample_tasks(successful_tasks),
            'unsuccessful_tasks': downsample_tasks(unsuccessful_tasks),
            'workers': workers,
            'x_domain': [0, current_app.config["RUNTIME_STATE"].MAX_TIME - current_app.config["RUNTIME_STATE"].MIN_TIME],
            'x_tick_values': compute_linear_tick_values([0, current_app.config["RUNTIME_STATE"].MAX_TIME - current_app.config["RUNTIME_STATE"].MIN_TIME]),
            'x_tick_formatter': d3_time_formatter(),
            'y_domain': y_domain,
            'y_tick_values': y_tick_values,
            'y_tick_formatter': d3_worker_core_formatter()
        }

        return jsonify(data)

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_execution_details: {e}")
        return jsonify({'error': str(e)}), 500

@task_execution_details_bp.route('/task-execution-details/export-csv')
@check_and_reload_data()
def export_task_execution_details_csv():
    try:
        rows = []

        for task in current_app.config["RUNTIME_STATE"].tasks.values():
            if not task.core_id:
                continue

            if task.task_status == 0:
                if not task.when_retrieved or task.is_library_task:
                    continue

                rows.append({
                    'type': 'Task Committing',
                    'start_time(s)': round(task.when_running - current_app.config["RUNTIME_STATE"].MIN_TIME, 2),
                    'end_time(s)': round(task.time_worker_start - current_app.config["RUNTIME_STATE"].MIN_TIME, 2),
                    'task_id': task.task_id,
                    'task_try_id': task.task_try_id
                })
                rows.append({
                    'type': 'Task Executing',
                    'start_time(s)': round(task.time_worker_start - current_app.config["RUNTIME_STATE"].MIN_TIME, 2),
                    'end_time(s)': round(task.time_worker_end - current_app.config["RUNTIME_STATE"].MIN_TIME, 2),
                    'task_id': task.task_id,
                    'task_try_id': task.task_try_id
                })
                rows.append({
                    'type': 'Task Retrieving',
                    'start_time(s)': round(task.time_worker_end - current_app.config["RUNTIME_STATE"].MIN_TIME, 2),
                    'end_time(s)': round(task.when_retrieved - current_app.config["RUNTIME_STATE"].MIN_TIME, 2),
                    'task_id': task.task_id,
                    'task_try_id': task.task_try_id
                })
            else:
                rows.append({
                    'type': 'Task Failed',
                    'start_time(s)': round(task.when_running - current_app.config["RUNTIME_STATE"].MIN_TIME, 2),
                    'end_time(s)': round(task.when_failure_happens - current_app.config["RUNTIME_STATE"].MIN_TIME, 2),
                    'task_id': task.task_id,
                    'task_try_id': task.task_try_id
                })

        for w in current_app.config["RUNTIME_STATE"].workers.values():
            if not w.hash:
                continue

            if len(w.time_disconnected) != len(w.time_connected):
                w.time_disconnected = [current_app.config["RUNTIME_STATE"].MAX_TIME] * (len(w.time_connected) - len(w.time_disconnected))

            for idx, (t0, t1) in enumerate(zip(w.time_connected, w.time_disconnected)):
                rows.append({
                    'type': 'Worker',
                    'start_time(s)': round(t0 - current_app.config["RUNTIME_STATE"].MIN_TIME, 2),
                    'end_time(s)': round(t1 - current_app.config["RUNTIME_STATE"].MIN_TIME, 2),
                    'worker_entry': f"{w.ip}:{w.port}:{w.connect_id}",
                })

        df = pd.DataFrame(rows)
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=task_execution_details.csv"
        response.headers["Content-Type"] = "text/csv"
        return response

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_task_execution_details_csv: {e}")
        return jsonify({'error': str(e)}), 500
