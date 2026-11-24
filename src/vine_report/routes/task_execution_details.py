from collections import defaultdict

from flask import Blueprint, jsonify, current_app
import polars as pl

from src.vine_report.request_context import get_run_snapshot
from src.utils import *

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
    # Tasks never dispatched or dispatch failed (no worker_entry/core_id, filtered from chart)
    42 << 3: 'unsuccessful-undispatched',
    43 << 3: 'unsuccessful-failed-to-dispatch',
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
    # Tasks without worker (never dispatched or dispatch failed)
    'unsuccessful-undispatched': ('Unsuccessful Tasks', 'Undispatched', '#9370DB'),
    'unsuccessful-failed-to-dispatch': ('Unsuccessful Tasks', 'Failed to Dispatch', '#8A2BE2'),
}

def calculate_legend(successful_tasks, unsuccessful_tasks, workers):
    # Metadata belongs to the request-pinned immutable snapshot.
    metadata = get_run_snapshot().metadata
    
    counts = defaultdict(int)

    # Successful task phases: use max(metadata, actual) so legend shows when we have
    # either metadata count or actual bars; ensures checkboxes exist for plotting
    successful_count = max(
        metadata.get('successful_tasks', 0),
        len(successful_tasks)
    )
    counts['successful-committing-to-worker'] = successful_count
    counts['successful-executing-on-worker'] = successful_count
    counts['successful-retrieving-to-manager'] = successful_count
    
    # Recovery tasks
    counts['recovery-successful'] = metadata.get('recovery_successful', 0)
    counts['recovery-unsuccessful'] = metadata.get('recovery_unsuccessful', 0)
    
    # Unsuccessful tasks by status
    # Note: metadata keys may be strings after JSON load, convert to int for lookup
    task_status_counts = metadata.get('task_status_counts', {})
    unknown_count = 0
    for status, count in task_status_counts.items():
        status_int = int(status) if isinstance(status, str) else status
        if status_int != 0:  # Not successful
            key = TASK_STATUS_TO_CHECKBOX_NAME.get(status_int)
            if key:
                counts[key] = count
            else:
                unknown_count += count
    if unknown_count > 0:
        counts['unsuccessful-unknown'] = counts.get('unsuccessful-unknown', 0) + unknown_count
    
    # Workers
    counts['workers'] = metadata.get('total_workers', 0)

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

    # Set group totals using metadata
    group_map['Successful Tasks']['total'] = metadata.get('successful_tasks', 0)
    group_map['Unsuccessful Tasks']['total'] = metadata.get('unsuccessful_tasks', 0)
    group_map['Recovery Tasks']['total'] = metadata.get('recovery_tasks', 0)
    group_map['Workers']['total'] = metadata.get('total_workers', 0)

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
        max_tasks = current_app.config["DOWNSAMPLE_TASK_BARS"]

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
        df = read_csv_frame(get_run_snapshot().csv_file_task_execution_details)
        
        successful_tasks = []
        unsuccessful_tasks = []
        workers = []
        
        # Process task data
        task_rows = df.filter(
            pl.col('record_type').is_in(['successful_tasks', 'unsuccessful_tasks'])
        )
        for row in task_rows.iter_rows(named=True):
            if is_missing_value(row['task_id']):
                continue

            base_task_data = {
                'task_id': int(row['task_id']),
                'try_id': int(row['task_try_id']),
                'worker_entry': str(row['worker_entry']),
                'worker_id': int(row['worker_id']),
                'core_id': int(row['core_id']),
                'cores_requested': task_execution_safe_int(row, 'cores_requested'),
                'is_recovery_task': bool(row['is_recovery_task']) if not is_missing_value(row['is_recovery_task']) else False,
                'input_files': str(row['input_files']) if not is_missing_value(row['input_files']) else '',
                'output_files': str(row['output_files']) if not is_missing_value(row['output_files']) else '',
                'num_input_files': task_execution_safe_int(row, 'num_input_files') or 0,
                'num_output_files': task_execution_safe_int(row, 'num_output_files') or 0,
                'task_status': task_execution_safe_int(row, 'task_status'),
                'category': str(row['category']) if not is_missing_value(row['category']) else '',
            }
            
            if row['record_type'] == 'successful_tasks':
                base_task_data.update({
                    key: task_execution_safe_float(row, key)
                    for key in (
                        'when_ready', 'when_running', 'time_worker_start',
                        'time_worker_end', 'execution_time',
                        'when_waiting_retrieval', 'when_retrieved', 'when_done'
                    )
                })
                successful_tasks.append(base_task_data)
            else:  # unsuccessful
                base_task_data.update({
                    **{
                        key: task_execution_safe_float(row, key)
                        for key in (
                            'when_ready', 'when_running', 'when_failure_happens',
                            'execution_time', 'when_done'
                        )
                    },
                    'unsuccessful_checkbox_name': str(row['unsuccessful_checkbox_name'])
                    if not is_missing_value(row['unsuccessful_checkbox_name']) else 'unknown',
                })
                unsuccessful_tasks.append(base_task_data)
        
        # Process worker data
        worker_rows = df.filter(pl.col('record_type') == 'worker')
        for row in worker_rows.iter_rows(named=True):
            if is_missing_value(row['worker_id']):
                continue

            workers.append({
                'hash': str(row['hash']) if not is_missing_value(row['hash']) else '',
                'id': int(row['worker_id']),
                'worker_entry': str(row['worker_entry']),
                'time_connected': parse_worker_time_list(row['time_connected']),
                'time_disconnected': parse_worker_time_list(row['time_disconnected']),
                'cores': task_execution_safe_int(row, 'cores') or 0,
                'memory_mb': task_execution_safe_int(row, 'memory_mb') or 0,
                'disk_mb': task_execution_safe_int(row, 'disk_mb') or 0,
                'gpus': task_execution_safe_int(row, 'gpus') or 0,
            })

        # Build y_domain from workers AND tasks (tasks may reference workers not in list, e.g. no hash)
        band_set = set()
        for w in workers:
            for i in range(1, max(1, w['cores']) + 1):
                band_set.add(f"{w['id']}-{i}")
        for t in successful_tasks + unsuccessful_tasks:
            band_set.add(f"{t['worker_id']}-{t['core_id']}")
        y_domain = sorted(band_set, key=lambda k: (int(k.split('-')[0]), int(k.split('-')[1])))

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
        get_run_snapshot().log_error(f"Error in get_task_execution_details: {e}")
        return jsonify({'error': str(e)}), 500
