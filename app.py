from flask import Flask, render_template, jsonify, Response, request, send_from_directory
import os
import argparse
import ast
import pandas as pd
from typing import Dict, Any
from pathlib import Path
from collections import defaultdict
import json
from data_parse import DataParser

LOGS_DIR = 'logs'


class TemplateState:
    def __init__(self):
        # full path to the runtime template
        self.runtime_template = None
        self.data_parser = None

        self.manager = None
        self.workers = None
        self.files = None
        self.tasks = None

        self.MIN_TIME = None
        self.MAX_TIME = None

        self.tick_size = 12

    def restore_from_checkpoint(self):
        self.manager, self.workers, self.files, self.tasks = self.data_parser.restore_from_checkpoint()

    def change_runtime_template(self, runtime_template):
        self.runtime_template = os.path.join(os.getcwd(), LOGS_DIR, Path(runtime_template).name)
        self.data_parser = DataParser(self.runtime_template)
        self.restore_from_checkpoint()
        self.MIN_TIME = float(self.manager.time_start)
        self.MAX_TIME = float(self.manager.time_end)

    def ensure_runtime_template(self, runtime_template):
        if not runtime_template:
            return
        if self.runtime_template and Path(runtime_template).name == Path(self.runtime_template).name:
            return
        self.change_runtime_template(runtime_template)


def all_subfolders_exists(parent: str, folder_names: list[str]) -> bool:
    parent_path = Path(parent).resolve()
    for folder_name in folder_names:
        target_path = parent_path / folder_name
        if not target_path.is_dir():
            return False
    return True


app = Flask(__name__)

@app.route('/api/execution-details')
def get_execution_details():
    try:
        data: Dict[str, Any] = {}

        data['xMin'] = 0
        data['xMax'] = template_manager.MAX_TIME - template_manager.MIN_TIME

        # tasks information
        data['doneTasks'] = []
        data['failedTasks'] = []
        for task in template_manager.tasks.values():
            if task.task_status == 0:
                done_task_info = {
                    'task_id': task.task_id,
                    'worker_ip': task.worker_ip,
                    'worker_port': task.worker_port,
                    'worker_id': task.worker_id,
                    'core_id': task.core_id[0],
                    'is_recovery_task': task.is_recovery_task,
                    'task_status': task.task_status,
                    'category': task.category,
                    'when_ready': task.when_ready - template_manager.MIN_TIME,
                    'when_running': task.when_running - template_manager.MIN_TIME,
                    'time_worker_start': task.time_worker_start - template_manager.MIN_TIME,
                    'time_worker_end': task.time_worker_end - template_manager.MIN_TIME,
                    'when_waiting_retrieval': task.when_waiting_retrieval - template_manager.MIN_TIME,
                    'when_retrieved': task.when_retrieved - template_manager.MIN_TIME,
                    'when_done': task.when_done - template_manager.MIN_TIME, 
                }
                data['doneTasks'].append(done_task_info)
            else:
                if len(task.core_id) == 0:    # not run at all
                    continue
                failed_task_info = {
                    'task_id': task.task_id,
                    'worker_ip': task.worker_ip,
                    'worker_port': task.worker_port,
                    'worker_id': task.worker_id,
                    'core_id': task.core_id[0],
                    'is_recovery_task': task.is_recovery_task,
                    'task_status': task.task_status,
                    'category': task.category,
                    'when_ready': task.when_ready - template_manager.MIN_TIME,
                    'when_running': task.when_running - template_manager.MIN_TIME,
                    'when_failure_happens': task.when_failure_happens - template_manager.MIN_TIME,
                }
                data['failedTasks'].append(failed_task_info)

        data['workerInfo'] = []
        for worker in template_manager.workers.values():
            if not worker.hash:
                continue
            worker_info = {
                'hash': worker.hash,
                'id': worker.id,
                'time_connected': [t - template_manager.MIN_TIME for t in worker.time_connected],
                'time_disconnected': [t - template_manager.MIN_TIME for t in worker.time_disconnected],
                'cores': worker.cores,
            }
            data['workerInfo'].append(worker_info)

        # ploting parameters
        data['tickFontSize'] = template_manager.tick_size
        data['xTickValues'] = [
            round(data['xMin'], 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.25, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.5, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.75, 2),
            round(data['xMax'], 2)
        ]

        return jsonify(data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

@app.route('/api/storage-consumption')
def get_storage_consumption():
    try:
        data = {}

        files = template_manager.files

        # construct the succeeded file transfers
        data['worker_storage_consumption'] = {}
        for file in files.values():
            for transfer in file.transfers:
                # skip if this is not a transfer to a worker
                if not isinstance(transfer.destination, tuple):
                    continue
                # skip if the transfer was not successful
                if transfer.time_stage_in is None or transfer.time_stage_out is None:
                    continue
                # add the transfer to the worker
                destination = transfer.destination
                if destination not in data['worker_storage_consumption']:
                    data['worker_storage_consumption'][destination] = []

                data['worker_storage_consumption'][destination].append((float(transfer.time_stage_in) - template_manager.MIN_TIME, file.size_mb))
                data['worker_storage_consumption'][destination].append((float(transfer.time_stage_out) - template_manager.MIN_TIME, -file.size_mb))

        max_storage_consumption = 0
        # sort the worker storage consumption
        for destination in data['worker_storage_consumption']:
            # convert to a pandas dataframe
            df = pd.DataFrame(data['worker_storage_consumption'][destination], columns=['time', 'size'])
            # sort the dataframe, time ascending, size descending
            df = df.sort_values(by=['time'])
            # accumulate the size
            df['storage_consumption'] = df['size'].cumsum()
            # group by time and keep the maximum storage consumption for each time
            df = df.groupby('time')['storage_consumption'].max().reset_index()
            # update the max storage consumption
            max_storage_consumption = max(max_storage_consumption, df['storage_consumption'].max())
            # keep only time and storage_consumption columns
            data['worker_storage_consumption'][destination] = df[['time', 'storage_consumption']].values.tolist()
            # add the initial point at time_connected with 0 consumption
            for time_connected, time_disconnected in zip(template_manager.workers[destination].time_connected, template_manager.workers[destination].time_disconnected):
                data['worker_storage_consumption'][destination].insert(0, [time_connected - template_manager.MIN_TIME, 0])
                data['worker_storage_consumption'][destination].append([time_disconnected - template_manager.MIN_TIME, 0])
            
        # convert the key to a string
        data['worker_storage_consumption'] = {f"{k[0]}:{k[1]}": v for k, v in data['worker_storage_consumption'].items()}

        # calculate the file size unit, the default is MB
        data['file_size_unit'] = 'MB'
        if max_storage_consumption < 1 / 1024:
            data['file_size_unit'] = 'Bytes'
            scale = 1024 * 1024           # times 1024 * 1024
        elif max_storage_consumption < 1:
            data['file_size_unit'] = 'KB'
            scale = 1024                  # times 1024
        elif max_storage_consumption > 1024:
            data['file_size_unit'] = 'GB'
            scale = 1 / 1024              # divide by 1024
        elif max_storage_consumption > 1024 * 1024:
            data['file_size_unit'] = 'TB'
            scale = 1 / (1024 * 1024)      # divide by 1024 * 1024
        else:
            scale = 1
        # also update the source data
        if scale != 1:
            for destination in data['worker_storage_consumption']:
                df = pd.DataFrame(data['worker_storage_consumption'][destination], columns=['time', 'size'])
                df['size'] = df['size'] * scale
                data['worker_storage_consumption'][destination] = df.values.tolist()
            max_storage_consumption *= scale

        # ploting parameters
        data['xMin'] = 0
        data['xMax'] = template_manager.MAX_TIME - template_manager.MIN_TIME
        data['yMin'] = 0
        data['yMax'] = max_storage_consumption
        data['xTickValues'] = [
            round(data['xMin'], 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.25, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.5, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.75, 2),
            round(data['xMax'], 2)
        ]
        data['yTickValues'] = [
            round(data['yMin'], 2),
            round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.25, 2),
            round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.5, 2),
            round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.75, 2),
            round(data['yMax'], 2)
        ]
        data['tickFontSize'] = template_manager.tick_size

        return jsonify(data)

    except Exception as e:
        print(f"Error in get_storage_consumption: {str(e)}")
        return jsonify({'error': str(e)}), 500
    

@app.route('/api/worker-transfers')
def get_worker_transfers():
    try:
        # Get the transfer type from query parameters
        transfer_type = request.args.get('type', 'incoming')  # default to incoming
        if transfer_type not in ['incoming', 'outgoing']:
            return jsonify({'error': 'Invalid transfer type'}), 400

        data = {}

        # construct the worker transfers
        data['transfers'] = defaultdict(list)     # for destinations/sources
        for file in template_manager.files.values():
            for transfer in file.transfers:
                destination = transfer.destination
                source = transfer.source

                # only consider worker-to-worker transfers
                if not isinstance(destination, tuple) or not isinstance(source, tuple):
                    continue

                # if transfer_type is incoming, process destinations
                if transfer_type == 'incoming' and isinstance(destination, tuple):
                    data['transfers'][destination].append((round(transfer.time_start_stage_in - template_manager.MIN_TIME, 2), 1))
                    if transfer.time_stage_in:
                        data['transfers'][destination].append((round(transfer.time_stage_in - template_manager.MIN_TIME, 2), -1))
                    elif transfer.time_stage_out:
                        data['transfers'][destination].append((round(transfer.time_stage_out - template_manager.MIN_TIME, 2), -1))
                # if transfer_type is outgoing, process sources
                elif transfer_type == 'outgoing' and isinstance(source, tuple):
                    data['transfers'][source].append((round(transfer.time_start_stage_in - template_manager.MIN_TIME, 2), 1))
                    if transfer.time_stage_in:
                        data['transfers'][source].append((round(transfer.time_stage_in - template_manager.MIN_TIME, 2), -1))
                    elif transfer.time_stage_out:
                        data['transfers'][source].append((round(transfer.time_stage_out - template_manager.MIN_TIME, 2), -1))

        max_transfers = 0
        for worker in data['transfers']:
            df = pd.DataFrame(data['transfers'][worker], columns=['time', 'event'])
            df = df.sort_values(by=['time'])
            df['cumulative_transfers'] = df['event'].cumsum()
            # if two rows have the same time, keep the one with the largest event
            df = df.drop_duplicates(subset=['time'], keep='last')
            data['transfers'][worker] = df[['time', 'cumulative_transfers']].values.tolist()
            # append the initial point at time_connected with 0
            for time_connected, time_disconnected in zip(template_manager.workers[worker].time_connected, template_manager.workers[worker].time_disconnected):
                data['transfers'][worker].insert(0, [time_connected - template_manager.MIN_TIME, 0])
                data['transfers'][worker].append([time_disconnected - template_manager.MIN_TIME, 0])
            max_transfers = max(max_transfers, df['cumulative_transfers'].max())

        # convert keys to string-formatted keys
        data['transfers'] = {f"{k[0]}:{k[1]}": v for k, v in data['transfers'].items()}

        # ploting parameters
        data['xMin'] = 0
        data['xMax'] = template_manager.MAX_TIME - template_manager.MIN_TIME
        data['yMin'] = 0
        data['yMax'] = max_transfers
        data['xTickValues'] = [
            round(data['xMin'], 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.25, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.5, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.75, 2),
            round(data['xMax'], 2)
        ]
        data['yTickValues'] = [
            int(data['yMin']),
            int(data['yMin'] + (data['yMax'] - data['yMin']) * 0.25),
            int(data['yMin'] + (data['yMax'] - data['yMin']) * 0.5),
            int(data['yMin'] + (data['yMax'] - data['yMin']) * 0.75),
            int(data['yMax'])
        ]
        data['tickFontSize'] = template_manager.tick_size
        return jsonify(data)

    except Exception as e:
        print(f"Error in get_worker_transfers: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/task-execution-time')
def get_task_execution_time():
    try:
        data = {}

        # get the execution time of each task
        data['task_execution_time'] = []
        for task in template_manager.tasks.values():
            # skip if the task didn't run to completion
            if task.task_status != 0:
                continue
            task_execution_time = round(task.time_worker_end - task.time_worker_start, 2)
            # if a task completes very quickly, we set it to 0.01
            task_execution_time = max(task_execution_time, 0.01)
            data['task_execution_time'].append((task.task_id, task_execution_time))

        # calculate the cdf of the task execution time using pandas, where the y is the probability
        df = pd.DataFrame(data['task_execution_time'], columns=['task_id', 'task_execution_time'])
        df = df.sort_values(by=['task_execution_time'])
        df['cumulative_execution_time'] = df['task_execution_time'].cumsum()
        df['probability'] = df['cumulative_execution_time'] / df['cumulative_execution_time'].max()
        df['probability'] = df['probability'].round(4)
        data['task_execution_time_cdf'] = df[['task_execution_time', 'probability']].values.tolist()

        # tick values
        num_tasks = len(df)
        data['execution_time_x_tick_values'] = [
            1,
            round(num_tasks * 0.25, 2),
            round(num_tasks * 0.5, 2),
            round(num_tasks * 0.75, 2),
            num_tasks
        ]
        max_execution_time = df['task_execution_time'].max()
        data['execution_time_y_tick_values'] = [
            0,
            round(max_execution_time * 0.25, 2),
            round(max_execution_time * 0.5, 2),
            round(max_execution_time * 0.75, 2),
            max_execution_time
        ]
        data['probability_y_tick_values'] = [0, 0.25, 0.5, 0.75, 1]
        data['probability_x_tick_values'] = [
            0,
            round(max_execution_time * 0.25, 2),
            round(max_execution_time * 0.5, 2),
            round(max_execution_time * 0.75, 2),
            max_execution_time
        ]

        data['tickFontSize'] = template_manager.tick_size

        return jsonify(data)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/task-concurrency')
def get_task_concurrency():
    try:
        data = {}
        
        # Get selected types from query parameter, default to all types if not specified
        selected_types = request.args.get('types', '').split(',')
        if not selected_types or selected_types == ['']:
            selected_types = [
                'tasks_waiting',
                'tasks_committing',
                'tasks_executing',
                'tasks_retrieving',
                'tasks_done'
            ]
        
        # Initialize all task types with empty lists in response
        all_task_types = {
            'tasks_waiting': [],
            'tasks_committing': [],
            'tasks_executing': [],
            'tasks_retrieving': [],
            'tasks_done': []
        }
        data.update(all_task_types)
        
        # Only process selected task types
        for task in template_manager.tasks.values():
            if task.when_failure_happens is not None:
                continue
            # waiting: when_ready -> when_running
            if 'tasks_waiting' in selected_types:
                if task.when_ready:
                    data['tasks_waiting'].append((task.when_ready - template_manager.MIN_TIME, 1))
                    if task.when_running:
                        data['tasks_waiting'].append((task.when_running - template_manager.MIN_TIME, -1))
            
            # committing: when_running -> time_worker_start
            if 'tasks_committing' in selected_types:
                if task.when_running:
                    data['tasks_committing'].append((task.when_running - template_manager.MIN_TIME, 1))
                    if task.time_worker_start:
                        data['tasks_committing'].append((task.time_worker_start - template_manager.MIN_TIME, -1))
            
            # executing: time_worker_start -> time_worker_end
            if 'tasks_executing' in selected_types:
                if task.time_worker_start:
                    data['tasks_executing'].append((task.time_worker_start - template_manager.MIN_TIME, 1))
                    if task.time_worker_end:
                        data['tasks_executing'].append((task.time_worker_end - template_manager.MIN_TIME, -1))
            
            # retrieving: time_worker_end -> when_retrieved
            if 'tasks_retrieving' in selected_types:
                if task.time_worker_end:
                    data['tasks_retrieving'].append((task.time_worker_end - template_manager.MIN_TIME, 1))
                    if task.when_retrieved:
                        data['tasks_retrieving'].append((task.when_retrieved - template_manager.MIN_TIME, -1))
            
            # done: when_retrieved -> when_done
            if 'tasks_done' in selected_types:
                if task.when_done:
                    data['tasks_done'].append((task.when_done - template_manager.MIN_TIME, 1))

            if task.when_failure_happens:

                data['tasks_done'].append((task.when_failure_happens - template_manager.MIN_TIME, 1))

        def sort_tasks(tasks):
            if not tasks:
                return []
            df = pd.DataFrame(tasks, columns=['time', 'event'])
            df = df.sort_values(by=['time'])
            df['time'] = df['time'].round(2)
            df['cumulative_event'] = df['event'].cumsum()
            # if two rows have the same time, keep the one with the largest event
            df = df.drop_duplicates(subset=['time'], keep='last')
            return df[['time', 'cumulative_event']].values.tolist()

        # Process all task types, but only calculate max for selected ones
        for task_type in all_task_types:
            data[task_type] = sort_tasks(data[task_type])

        # Calculate min/max values based only on selected data
        data['xMin'] = 0
        data['xMax'] = template_manager.MAX_TIME - template_manager.MIN_TIME
        data['yMin'] = 0
        
        # Calculate yMax only from selected types
        max_values = []
        for task_type in selected_types:
            if data[task_type]:
                max_values.extend([point[1] for point in data[task_type]])
        data['yMax'] = max(max_values) if max_values else 0

        # Generate tick values
        data['xTickValues'] = [
            round(data['xMin'], 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.25, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.5, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.75, 2),
            round(data['xMax'], 2)
        ]
        data['yTickValues'] = [
            round(data['yMin'], 2),
            round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.25, 2),
            round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.5, 2),
            round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.75, 2),
            round(data['yMax'], 2)
        ]
        data['tickFontSize'] = template_manager.tick_size

        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/runtime-templates-list')
def get_runtime_templates_list():
    log_folders = [name for name in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    valid_runtime_templates = []
    # for each log folder, we need to check if there is 'vine-logs' folder under it
    for log_folder in log_folders:
        if all_subfolders_exists(os.path.join(LOGS_DIR, log_folder), ['vine-logs', 'pkl-files']):
            valid_runtime_templates.append(log_folder)
    valid_runtime_templates = sorted(valid_runtime_templates)
    return jsonify(valid_runtime_templates)

@app.route('/api/change-runtime-template')
def change_runtime_template():
    runtime_template = request.args.get('runtime_template')
    template_manager.change_runtime_template(runtime_template)
    return jsonify({'success': True})

@app.route('/logs/<runtime_template>')
def render_log_page(runtime_template):
    log_folders = [name for name in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    log_folders_sorted = sorted(log_folders)
    if runtime_template != template_manager.runtime_template:
        template_manager.change_runtime_template(runtime_template)
    return render_template('index.html', log_folders=log_folders_sorted)

@app.route('/')
def index():
    log_folders = [name for name in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    log_folders_sorted = sorted(log_folders)
    return render_template('index.html', log_folders=log_folders_sorted)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default=9122, help='Port number')
    args = parser.parse_args()

    template_manager = TemplateState()
    
    app.run(host='0.0.0.0', port=args.port, debug=True)
