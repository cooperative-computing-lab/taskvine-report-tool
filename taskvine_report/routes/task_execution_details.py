from taskvine_report.utils import *

import pandas as pd
from collections import defaultdict
from flask import Blueprint, jsonify, current_app

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
        # Read data from CSV file
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_task_execution_details)
        
        successful_tasks = []
        unsuccessful_tasks = []
        workers = []
        
        # Process task data
        task_rows = df[df['record_type'].isin(['successful_tasks', 'unsuccessful_tasks'])]
        for _, row in task_rows.iterrows():
            if pd.isna(row['task_id']):
                continue

            base_task_data = {
                'task_id': int(row['task_id']),
                'try_id': int(row['task_try_id']),
                'worker_entry': str(row['worker_entry']),
                'worker_id': int(row['worker_id']),
                'core_id': int(row['core_id']),
                'is_recovery_task': bool(row['is_recovery_task']),
                'input_files': str(row['input_files']) if pd.notna(row['input_files']) else '',
                'output_files': str(row['output_files']) if pd.notna(row['output_files']) else '',
                'num_input_files': int(row['num_input_files']) if pd.notna(row['num_input_files']) else 0,
                'num_output_files': int(row['num_output_files']) if pd.notna(row['num_output_files']) else 0,
                'task_status': int(row['task_status']) if pd.notna(row['task_status']) else None,
                'category': str(row['category']) if pd.notna(row['category']) else '',
            }
            
            if row['record_type'] == 'successful_tasks':
                base_task_data.update({
                    'when_ready': float(row['when_ready']) if pd.notna(row['when_ready']) else None,
                    'when_running': float(row['when_running']) if pd.notna(row['when_running']) else None,
                    'time_worker_start': float(row['time_worker_start']) if pd.notna(row['time_worker_start']) else None,
                    'time_worker_end': float(row['time_worker_end']) if pd.notna(row['time_worker_end']) else None,
                    'execution_time': float(row['execution_time']) if pd.notna(row['execution_time']) else None,
                    'when_waiting_retrieval': float(row['when_waiting_retrieval']) if pd.notna(row['when_waiting_retrieval']) else None,
                    'when_retrieved': float(row['when_retrieved']) if pd.notna(row['when_retrieved']) else None,
                    'when_done': float(row['when_done']) if pd.notna(row['when_done']) else 'N/A'
                })
                successful_tasks.append(base_task_data)
            else:  # unsuccessful
                base_task_data.update({
                    'when_ready': float(row['when_ready']) if pd.notna(row['when_ready']) else None,
                    'when_running': float(row['when_running']) if pd.notna(row['when_running']) else None,
                    'when_failure_happens': float(row['when_failure_happens']) if pd.notna(row['when_failure_happens']) else None,
                    'execution_time': float(row['execution_time']) if pd.notna(row['execution_time']) else None,
                    'unsuccessful_checkbox_name': str(row['unsuccessful_checkbox_name']) if pd.notna(row['unsuccessful_checkbox_name']) else 'unknown',
                    'when_done': float(row['when_done']) if pd.notna(row['when_done']) else 'N/A'
                })
                unsuccessful_tasks.append(base_task_data)
        
        # Process worker data
        worker_rows = df[df['record_type'] == 'worker']
        for _, row in worker_rows.iterrows():
            if pd.isna(row['worker_id']):
                continue
                
            # Parse time_connected and time_disconnected arrays
            time_connected = []
            time_disconnected = []
            
            if pd.notna(row['time_connected']):
                try:
                    time_connected = eval(row['time_connected'])  # Convert string representation to list
                except:
                    time_connected = []
                    
            if pd.notna(row['time_disconnected']):
                try:
                    time_disconnected = eval(row['time_disconnected'])  # Convert string representation to list
                except:
                    time_disconnected = []
                    
            workers.append({
                'hash': str(row['hash']) if pd.notna(row['hash']) else '',
                'id': int(row['worker_id']),
                'worker_entry': str(row['worker_entry']),
                'time_connected': time_connected,
                'time_disconnected': time_disconnected,
                'cores': int(row['cores']) if pd.notna(row['cores']) else 0,
                'memory_mb': int(row['memory_mb']) if pd.notna(row['memory_mb']) else 0,
                'disk_mb': int(row['disk_mb']) if pd.notna(row['disk_mb']) else 0,
                'gpus': int(row['gpus']) if pd.notna(row['gpus']) else 0
            })

        # Calculate y_domain and y_tick_values
        y_domain = [f"{w['id']}-{i}" for w in workers for i in range(1, w['cores'] + 1)]
        if len(y_domain) <= 5:
            y_tick_values = y_domain
        else:
            num_ticks = 5
            step = (len(y_domain) - 1) / (num_ticks - 1)
            indices = [round(i * step) for i in range(num_ticks)]
            y_tick_values = [y_domain[i] for i in indices]

        x_domain = get_current_time_domain()
        
        data = {
            'legend': calculate_legend(successful_tasks, unsuccessful_tasks, workers),
            'successful_tasks': downsample_tasks(successful_tasks),
            'unsuccessful_tasks': downsample_tasks(unsuccessful_tasks),
            'workers': workers,
            'x_domain': x_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_domain': y_domain,
            'y_tick_values': y_tick_values,
            'y_tick_formatter': d3_worker_core_formatter()
        }

        return jsonify(data)

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_execution_details: {e}")
        return jsonify({'error': str(e)}), 500
