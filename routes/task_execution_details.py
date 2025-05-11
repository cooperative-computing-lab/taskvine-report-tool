from .runtime_state import runtime_state, SAMPLING_TASK_BARS, check_and_reload_data
from .utils import compute_tick_values

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

def calculate_legend(data):
    counts = defaultdict(int)

    for task in data['successful_tasks']:
        counts['successful-committing-to-worker'] += 1
        counts['successful-executing-on-worker'] += 1
        counts['successful-retrieving-to-manager'] += 1
        if task['is_recovery_task']:
            counts['recovery-successful'] += 1

    for task in data['unsuccessful_tasks']:
        key = TASK_STATUS_TO_CHECKBOX_NAME.get(task['task_status'])
        if key:
            counts[key] += 1
            if task['is_recovery_task']:
                counts['recovery-unsuccessful'] += 1

    counts['workers'] = len(data['worker_info'])

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
        group_map[group]['total'] = len(data['successful_tasks'] if group == 'Successful Tasks' else data['unsuccessful_tasks'])

    for group in ['Recovery Tasks', 'Workers']:
        group_map[group]['total'] = sum(item['count'] for item in group_map[group]['items'])

    group_order = ['Successful Tasks', 'Unsuccessful Tasks', 'Recovery Tasks', 'Workers']
    legend = []
    for group in group_order:
        if group not in group_map:
            group_map[group] = {'total': 0, 'items': []}
        legend.append({
            'group': group,
            'total': group_map[group]['total'],
            'items': group_map[group]['items']
        })

    return legend

@task_execution_details_bp.route('/task-execution-details')
@check_and_reload_data()
def get_task_execution_details():
    try:
        data = {}

        data['x_min'] = 0
        data['x_max'] = runtime_state.MAX_TIME - runtime_state.MIN_TIME

        # prepare task information
        data['successful_tasks'] = []
        data['unsuccessful_tasks'] = []
        data['num_of_status'] = defaultdict(int)
        data['num_successful_recovery_tasks'] = 0
        data['num_unsuccessful_recovery_tasks'] = 0
        for task in runtime_state.tasks.values():
            if task.task_status == 0:
                # skip tasks not retrieved or library tasks
                if not task.when_retrieved:
                    continue
                if task.is_library_task:
                    continue
                if len(task.core_id) == 0:
                    raise ValueError(f"Task {task.task_id} has no core_id, but when_running is {task.when_running}, when_failure_happens is {task.when_failure_happens}, when_waiting_retrieval is {task.when_waiting_retrieval}, when_retrieved is {task.when_retrieved}, when_done is {task.when_done}")
                data['num_of_status'][task.task_status] += 1
                if task.is_recovery_task:
                    data['num_successful_recovery_tasks'] += 1
                done_task_info = {
                    'task_id': task.task_id,
                    'worker_ip': task.worker_ip,
                    'worker_port': task.worker_port,
                    'worker_id': task.worker_id,
                    'core_id': task.core_id[0],
                    'is_recovery_task': task.is_recovery_task,
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
                }
                data['successful_tasks'].append(done_task_info)
            else:
                # skip tasks not assigned to any core
                if len(task.core_id) == 0:
                    continue
                if task.is_recovery_task:
                    data['num_unsuccessful_recovery_tasks'] += 1
                data['num_of_status'][task.task_status] += 1
                unsuccessful_task_info = {
                    'task_id': task.task_id,
                    'worker_ip': task.worker_ip,
                    'worker_port': task.worker_port,
                    'worker_id': task.worker_id,
                    'core_id': task.core_id[0],
                    'is_recovery_task': task.is_recovery_task,
                    'num_input_files': len(task.input_files),
                    'num_output_files': len(task.output_files),
                    'task_status': task.task_status,
                    'category': task.category,
                    'when_ready': task.when_ready - runtime_state.MIN_TIME,
                    'when_running': task.when_running - runtime_state.MIN_TIME,
                    'when_failure_happens': task.when_failure_happens - runtime_state.MIN_TIME,
                    'execution_time': task.when_failure_happens - task.when_running,
                    'unsuccessful_checkbox_name': TASK_STATUS_TO_CHECKBOX_NAME[task.task_status]
                }
                data['unsuccessful_tasks'].append(unsuccessful_task_info)

        # worker information
        data['worker_info'] = []
        for worker in runtime_state.workers.values():
            if not worker.hash:
                continue
            # handle workers with missing disconnect time
            if len(worker.time_disconnected) != len(worker.time_connected):
                worker.time_disconnected = [
                    runtime_state.MAX_TIME] * (len(worker.time_connected) - len(worker.time_disconnected))
            worker_info = {
                'hash': worker.hash,
                'id': worker.id,
                'worker_ip_port': f"{worker.ip}:{worker.port}",
                'time_connected': [max(t - runtime_state.MIN_TIME, 0) for t in worker.time_connected],
                'time_disconnected': [max(t - runtime_state.MIN_TIME, 0) for t in worker.time_disconnected],
                'cores': worker.cores,
                'memory_mb': worker.memory_mb,
                'disk_mb': worker.disk_mb,
                'gpus': worker.gpus,
            }
            data['worker_info'].append(worker_info)

        # calculate legend
        data['legend'] = calculate_legend(data)

        # limit to top tasks by execution time if needed
        if len(data['successful_tasks']) > SAMPLING_TASK_BARS:
            data['successful_tasks'] = sorted(data['successful_tasks'],
                                             key=lambda x: x['execution_time'],
                                             reverse=True)[:SAMPLING_TASK_BARS]
        if len(data['unsuccessful_tasks']) > SAMPLING_TASK_BARS:
            data['unsuccessful_tasks'] = sorted(data['unsuccessful_tasks'],
                                               key=lambda x: x['execution_time'],
                                               reverse=True)[:SAMPLING_TASK_BARS]

        # calculate x-axis domain and ticks
        data['x_domain'] = [0, float(runtime_state.MAX_TIME - runtime_state.MIN_TIME)]
        data['x_tick_values'] = compute_tick_values(data['x_domain'])

        # calculate y-axis domain and ticks
        data['y_domain'] = []
        for w in data['worker_info']:
            data['y_domain'].extend([f"{w['id']}-{i}" for i in range(1, w['cores'] + 1)])

        n = len(data['y_domain'])
        if n <= 5:
            data['y_tick_values'] = data['y_domain']
        else:
            indices = [round(i) for i in linspace(0, n - 1, 5)]
            data['y_tick_values'] = [data['y_domain'][i] for i in indices]

        return jsonify(data)

    except Exception as e:
        error_message = ''.join(
            traceback.format_exception(type(e), e, e.__traceback__))
        print(error_message)
        return jsonify({'error': str(e), 'details': error_message}), 500
