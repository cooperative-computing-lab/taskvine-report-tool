from .runtime_state import runtime_state, SAMPLING_TASK_BARS, check_and_reload_data
from .utils import (
    compute_tick_values,
    d3_time_formatter,
    d3_worker_core_formatter,
    file_list_formatter,
)

import traceback
from collections import defaultdict
from flask import Blueprint, jsonify
from numpy import linspace

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
    'workers': ('Workers', 'Workers', 'lightgrey', True),

    'successful-committing-to-worker': ('Successful Tasks', 'Committing', '#4a4a4a', False),
    'successful-executing-on-worker':  ('Successful Tasks', 'Executing', 'steelblue', True),
    'successful-retrieving-to-manager': ('Successful Tasks', 'Retrieving', '#cc5a12', False),

    'recovery-successful': ('Recovery Tasks', 'Successful', '#FF69B4', False),
    'recovery-unsuccessful': ('Recovery Tasks', 'Unsuccessful', '#E3314F', False),

    'unsuccessful-input-missing': ('Unsuccessful Tasks', 'Input Missing', '#FFB6C1', False),
    'unsuccessful-output-missing': ('Unsuccessful Tasks', 'Output Missing', '#FF69B4', False),
    'unsuccessful-stdout-missing': ('Unsuccessful Tasks', 'Stdout Missing', '#FF1493', False),
    'unsuccessful-signal': ('Unsuccessful Tasks', 'Signal', '#CD5C5C', False),
    'unsuccessful-resource-exhaustion': ('Unsuccessful Tasks', 'Resource Exhaustion', '#8B0000', False),
    'unsuccessful-max-end-time': ('Unsuccessful Tasks', 'Max End Time', '#B22222', False),
    'unsuccessful-unknown': ('Unsuccessful Tasks', 'Unknown', '#A52A2A', False),
    'unsuccessful-forsaken': ('Unsuccessful Tasks', 'Forsaken', '#E331EE', False),
    'unsuccessful-max-retries': ('Unsuccessful Tasks', 'Max Retries', '#8B4513', False),
    'unsuccessful-max-wall-time': ('Unsuccessful Tasks', 'Max Wall Time', '#D2691E', False),
    'unsuccessful-monitor-error': ('Unsuccessful Tasks', 'Monitor Error', '#FF4444', False),
    'unsuccessful-output-transfer-error': ('Unsuccessful Tasks', 'Output Transfer Error', '#FF6B6B', False),
    'unsuccessful-location-missing': ('Unsuccessful Tasks', 'Location Missing', '#FF8787', False),
    'unsuccessful-cancelled': ('Unsuccessful Tasks', 'Cancelled', '#FFA07A', False),
    'unsuccessful-library-exit': ('Unsuccessful Tasks', 'Library Exit', '#FA8072', False),
    'unsuccessful-sandbox-exhaustion': ('Unsuccessful Tasks', 'Sandbox Exhaustion', '#E9967A', False),
    'unsuccessful-missing-library': ('Unsuccessful Tasks', 'Missing Library', '#F08080', False),
    'unsuccessful-worker-disconnected': ('Unsuccessful Tasks', 'Worker Disconnected', '#FF0000', False),
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
    for key, (group, label, color, default_checked) in LEGEND_SCHEMA.items():
        count = counts.get(key, 0)
        if count == 0:
            continue
        group_map[group]['items'].append({
            'id': key,
            'label': label,
            'count': count,
            'color': color,
            'default_checked': default_checked
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

def downsample_tasks(tasks, key="execution_time", max_tasks=SAMPLING_TASK_BARS):
    if len(tasks) <= max_tasks:
        return tasks

    # sort tasks by execution time
    sorted_tasks = sorted(tasks, key=lambda x: x[key], reverse=True)

    return sorted_tasks[:max_tasks]

@task_execution_details_bp.route('/task-execution-details')
@check_and_reload_data()
def get_task_execution_details():
    try:
        successful_tasks = []
        unsuccessful_tasks = []
        workers = []

        for task in runtime_state.tasks.values():
            if not task.core_id:
                continue

            if task.task_status == 0:
                if not task.when_retrieved or task.is_library_task:
                    continue

                successful_tasks.append({
                    'task_id': task.task_id,
                    'try_id': task.try_id,
                    'worker_ip': task.worker_ip,
                    'worker_port': task.worker_port,
                    'worker_id': task.worker_id,
                    'core_id': task.core_id[0],
                    'is_recovery_task': task.is_recovery_task,
                    'input_files': file_list_formatter(task.input_files),
                    'output_files': file_list_formatter(task.output_files),
                    'num_input_files': len(task.input_files),
                    'num_output_files': len(task.output_files),
                    'task_status': task.task_status,
                    'category': task.category,
                    'when_ready': task.when_ready - runtime_state.MIN_TIME,
                    'when_running': task.when_running - runtime_state.MIN_TIME,
                    'time_worker_start': task.time_worker_start - runtime_state.MIN_TIME,
                    'time_worker_end': task.time_worker_end - runtime_state.MIN_TIME,
                    'execution_time': task.time_worker_end - task.time_worker_start,
                    'when_waiting_retrieval': task.when_waiting_retrieval - runtime_state.MIN_TIME,
                    'when_retrieved': task.when_retrieved - runtime_state.MIN_TIME,
                    'when_done': task.when_done - runtime_state.MIN_TIME if task.when_done else -1
                })
            else:
                unsuccessful_tasks.append({
                    'task_id': task.task_id,
                    'try_id': task.try_id,
                    'worker_ip': task.worker_ip,
                    'worker_port': task.worker_port,
                    'worker_id': task.worker_id,
                    'core_id': task.core_id[0],
                    'is_recovery_task': task.is_recovery_task,
                    'input_files': file_list_formatter(task.input_files),
                    'output_files': file_list_formatter(task.output_files),
                    'num_input_files': len(task.input_files),
                    'num_output_files': len(task.output_files),
                    'task_status': task.task_status,
                    'category': task.category,
                    'when_ready': task.when_ready - runtime_state.MIN_TIME,
                    'when_running': task.when_running - runtime_state.MIN_TIME,
                    'when_failure_happens': task.when_failure_happens - runtime_state.MIN_TIME,
                    'execution_time': task.when_failure_happens - task.when_running,
                    'unsuccessful_checkbox_name': TASK_STATUS_TO_CHECKBOX_NAME.get(task.task_status, 'unknown'),
                    'when_done': task.when_done - runtime_state.MIN_TIME if task.when_done else -1
                })

        for w in runtime_state.workers.values():
            if not w.hash:
                continue

            if len(w.time_disconnected) != len(w.time_connected):
                w.time_disconnected = [runtime_state.MAX_TIME] * (len(w.time_connected) - len(w.time_disconnected))

            workers.append({
                'hash': w.hash,
                'id': w.id,
                'worker_ip_port': f"{w.ip}:{w.port}",
                'time_connected': [max(t - runtime_state.MIN_TIME, 0) for t in w.time_connected],
                'time_disconnected': [max(t - runtime_state.MIN_TIME, 0) for t in w.time_disconnected],
                'cores': w.cores,
                'memory_mb': w.memory_mb,
                'disk_mb': w.disk_mb,
                'gpus': w.gpus
            })

        y_domain = [f"{w['id']}-{i}" for w in workers for i in range(1, w['cores'] + 1)]
        if len(y_domain) <= 5:
            y_tick_values = y_domain
        else:
            indices = [round(i) for i in linspace(0, len(y_domain) - 1, 5)]
            y_tick_values = [y_domain[i] for i in indices]

        data = {
            'legend': calculate_legend(successful_tasks, unsuccessful_tasks, workers),
            'successful_tasks': downsample_tasks(successful_tasks),
            'unsuccessful_tasks': downsample_tasks(unsuccessful_tasks),
            'workers': workers,
            'x_domain': [0, runtime_state.MAX_TIME - runtime_state.MIN_TIME],
            'x_tick_values': compute_tick_values([0, runtime_state.MAX_TIME - runtime_state.MIN_TIME]),
            'x_tick_formatter': d3_time_formatter(),
            'y_domain': y_domain,
            'y_tick_values': y_tick_values,
            'y_tick_formatter': d3_worker_core_formatter()
        }

        return jsonify(data)

    except Exception as e:
        err_msg = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        print(err_msg)
        return jsonify({'error': str(e), 'details': err_msg}), 500