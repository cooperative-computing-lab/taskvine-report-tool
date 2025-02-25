from flask import Flask, render_template, jsonify, Response, request, send_from_directory
import os
import argparse
import pandas as pd
from typing import Dict, Any
from pathlib import Path
from collections import defaultdict
import graphviz
from src.data_parse import DataParser

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
        self.subgraphs = None

        # for storing the graph files
        self.svg_files_dir = None   

        self.MIN_TIME = None
        self.MAX_TIME = None

        self.tick_size = 12

    def restore_from_checkpoint(self):
        self.data_parser.restore_from_checkpoint()
        self.manager = self.data_parser.manager
        self.workers = self.data_parser.workers
        self.files = self.data_parser.files
        self.tasks = self.data_parser.tasks
        self.subgraphs = self.data_parser.subgraphs

    def change_runtime_template(self, runtime_template):
        self.runtime_template = os.path.join(os.getcwd(), LOGS_DIR, Path(runtime_template).name)
        self.data_parser = DataParser(self.runtime_template)
        self.svg_files_dir = self.data_parser.svg_files_dir
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
        data['successfulTasks'] = []
        data['unsuccessfulTasks'] = []
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
                data['successfulTasks'].append(done_task_info)
            else:
                if len(task.core_id) == 0:    # not run at all
                    continue
                unsuccessful_task_info = {
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
                data['unsuccessfulTasks'].append(unsuccessful_task_info)

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

        data['file_size_unit'], scale = get_unit_and_scale_by_max_file_size_mb(max_storage_consumption)

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
        data['yMax'] = int(max_transfers)
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
        data['tickFontSize'] = int(template_manager.tick_size)
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
    
# calculate the file size unit, the default is MB
def get_unit_and_scale_by_max_file_size_mb(max_file_size_mb) -> tuple[str, float]:
    if max_file_size_mb < 1 / 1024:
        return 'Bytes',  1024 * 1024
    elif max_file_size_mb < 1:
        return 'KB', 1024
    elif max_file_size_mb > 1024:
        return 'GB', 1 / 1024
    elif max_file_size_mb > 1024 * 1024:
        return 'TB', 1 / (1024 * 1024)
    else:
        return 'MB', 1
    
@app.route('/api/file-replicas')
def get_file_replicas():
    try:
        order = request.args.get('order', 'desc')  # default to descending
        if order not in ['asc', 'desc']:
            return jsonify({'error': 'Invalid order'}), 400

        data = {}
        
        # Get the file size of each file
        data['file_replicas'] = []
        for file in template_manager.files.values():
            # skip if the file was not staged in at all (outfile of a task but task unsuccessful)
            file_name = file.filename
            file_size = file.size_mb
            if len(file.transfers) == 0:
                continue
            # skip if not a temp file
            if not file_name.startswith('temp-'):
                continue
            workers = set()
            for transfer in file.transfers:
                # skip if the file is not staged in
                if not transfer.time_stage_in:
                    continue
                workers.add(transfer.destination)
            data['file_replicas'].append((0, file_name, file_size, len(workers)))

        # sort the file replicas using pandas
        df = pd.DataFrame(data['file_replicas'], columns=['file_idx', 'file_name', 'file_size', 'num_replicas'])
        if order == 'asc':
            df = df.sort_values(by=['num_replicas'])
        elif order == 'desc':   
            df = df.sort_values(by=['num_replicas'], ascending=False)
        df['file_idx'] = range(1, len(df) + 1)
        
        # convert the dataframe to a list of tuples
        data['file_replicas'] = df.values.tolist()
        
        # ploting parameters
        if len(df) == 0:
            data['xMin'] = 1
            data['xMax'] = 1
            data['yMin'] = 0    
            data['yMax'] = 0
        else:
            data['xMin'] = 1
            data['xMax'] = len(df)
            data['yMin'] = 0    
            data['yMax'] = df['num_replicas'].max()
        data['xTickValues'] = [
            round(data['xMin'], 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.25, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.5, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.75, 2),
            round(data['xMax'], 2)
            ]
        data['yTickValues'] = [
            round(data['yMin'], 2),
            int(round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.25, 2)),
            int(round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.5, 2)),
            int(round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.75, 2)),
            int(round(data['yMax'], 2))
            ]
        data['tickFontSize'] = template_manager.tick_size

        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/file-sizes')
def get_file_sizes():
    try:
        # Get the transfer type from query parameters
        order = request.args.get('order', 'asc')  # default to ascending
        file_type = request.args.get('type', 'all')  # default to all
        if order not in ['asc', 'desc', 'created-time']:
            return jsonify({'error': 'Invalid order'}), 400
        if file_type not in ['temp', 'meta', 'buffer', 'task-created', 'transferred', 'all']:
            return jsonify({'error': 'Invalid file type'}), 400
        
        data = {}
        
        # Get the file size of each file
        data['file_sizes'] = []
        max_file_size_mb = 0
        for file in template_manager.files.values():
            # skip if the file was not staged in at all (outfile of a task but task unsuccessful)
            if len(file.transfers) == 0:
                continue
            file_name = file.filename
            file_size = file.size_mb
            if file_type != 'all':
                if file_type == 'temp' and not file_name.startswith('temp-'):
                    continue
                if file_type == 'meta' and not file_name.startswith('file-meta'):
                    continue
                if file_type == 'buffer' and not file_name.startswith('buffer-'):
                    continue
                if file_type == 'task-created' and len(file.producers) == 0:
                    continue
                if file_type == 'transferred' and len(file.transfers) == 0:
                    continue
            file_created_time = float('inf')
            for transfer in file.transfers:
                file_created_time = round(min(file_created_time, transfer.time_start_stage_in - template_manager.MIN_TIME), 2)
            if file_created_time == float('inf'):
                print(f"Warning: file {file_name} has no transfer")
            data['file_sizes'].append((0, file_name, file_size, file_created_time))
            max_file_size_mb = max(max_file_size_mb, file_size)

        # sort the file sizes using pandas
        df = pd.DataFrame(data['file_sizes'], columns=['file_idx', 'file_name', 'file_size', 'file_created_time'])
        if order == 'asc':
            df = df.sort_values(by=['file_size'])
        elif order == 'desc':
            df = df.sort_values(by=['file_size'], ascending=False)
        elif order == 'created-time':
            df = df.sort_values(by=['file_created_time'])

        # file idx should start from 1
        df['file_idx'] = range(1, len(df) + 1)
        # convert the file size to the desired unit
        data['file_size_unit'], scale = get_unit_and_scale_by_max_file_size_mb(max_file_size_mb)
        df['file_size'] = df['file_size'] * scale
        
        # convert the dataframe to a list of tuples
        data['file_sizes'] = df.values.tolist()

        # ploting parameters
        if len(df) == 0:
            data['xMin'] = 1
            data['xMax'] = 1
            data['yMin'] = 0    
            data['yMax'] = 0
        else:
            data['xMin'] = 1
            data['xMax'] = len(df)
            data['yMin'] = 0    
            data['yMax'] = max_file_size_mb * scale
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
        data['file_size_unit'] = data['file_size_unit']

        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/subgraphs')
def get_subgraphs():
    try:
        data = {}
        
        subgraph_id = request.args.get('subgraph_id')
        if not subgraph_id:
            return jsonify({'error': 'Subgraph ID is required'}), 400
        subgraph_id = int(subgraph_id)
        if subgraph_id not in template_manager.subgraphs.keys():
            return jsonify({'error': 'Invalid subgraph ID'}), 400
        
        plot_unsuccessful_task = request.args.get('plot_unsuccessful_task', 'true').lower() == 'true'
        plot_recovery_task = request.args.get('plot_recovery_task', 'true').lower() == 'true'

        subgraph = template_manager.subgraphs[subgraph_id]

        def plot_task_node(dot, task):
            node_id = f'{task.task_id}-{task.task_try_id}'
            node_label = f'{task.task_id}'

            if task.when_failure_happens:
                node_label = f'{node_label} (unsuccessful)'
                style = 'dashed'
                color = '#FF0000'
                fontcolor = '#FF0000'
            else:
                style = 'solid'
                color = '#000000'      # black border
                fontcolor = '#000000'  # black text

            if task.is_recovery_task:
                node_label = f'{node_label} (recovery)'
                style = 'filled,dashed'
                fillcolor = '#FF69B4'
            else:
                fillcolor = '#FFFFFF'  # white background

            dot.node(node_id, node_label, shape='ellipse', style=style, color=color, fontcolor=fontcolor, fillcolor=fillcolor)

        def plot_file_node(dot, file):
            # skip if the file was not produced by any task
            if len(file.producers) == 0:
                return
            file_name = file.filename
            dot.node(file_name, file_name, shape='box')

        def plot_task2file_edge(dot, task, file):
            # skip if the file was not produced by any task
            if len(file.producers) == 0:
                return
            # skip if the task unsuccessful (a unsuccessful task can't produce any file)
            if task.when_failure_happens:
                return
            else:
                task_execution_time = task.time_worker_end - task.time_worker_start
                dot.edge(f'{task.task_id}-{task.task_try_id}', file.filename, label=f'{task_execution_time:.2f}s')
        
        def plot_file2task_edge(dot, file, task):
            # skip if the file was not produced by any task
            if len(file.producers) == 0:
                return
            # file_creation_time = task.when_running - file.producers[0].time_worker_end
            file_creation_time = float('inf')
            for producer_task_id, producer_task_try_id in file.producers:
                producer_task = template_manager.tasks[(producer_task_id, producer_task_try_id)]
                if producer_task.time_worker_end:
                    file_creation_time = min(file_creation_time, producer_task.time_worker_end)
            file_creation_time = file_creation_time - template_manager.MIN_TIME

            if task.when_failure_happens:
                dot.edge(file.filename, f'{task.task_id}-{task.task_try_id}', label=f'{file_creation_time:.2f}s')
            else:
                dot.edge(file.filename, f'{task.task_id}-{task.task_try_id}', label=f'{file_creation_time:.2f}s')

        svg_file_path_without_suffix = os.path.join(template_manager.svg_files_dir, f'subgraph-{subgraph_id}-{plot_unsuccessful_task}-{plot_recovery_task}')
        svg_file_path = f'{svg_file_path_without_suffix}.svg'

        if not Path(svg_file_path).exists():
            dot = graphviz.Digraph()
            for (task_id, task_try_id) in list(subgraph):
                task = template_manager.tasks[(task_id, task_try_id)]
                # skip if the task is a recovery task and we don't want to plot recovery task
                if task.is_recovery_task and not plot_recovery_task:
                    continue
                # skip if the task unsuccessful and we don't want to plot unsuccessful task
                if task.when_failure_happens and not plot_unsuccessful_task:
                    continue
                # task node
                plot_task_node(dot, task)
                # input files
                for file_name in task.input_files:
                    file = template_manager.files[file_name]
                    plot_file_node(dot, file)
                    plot_file2task_edge(dot, file, task)
                # output files
                for file_name in task.output_files:
                    file = template_manager.files[file_name]
                    # do not plot if the file has not been created
                    if len(file.transfers) == 0:
                        continue
                    plot_file_node(dot, file)
                    plot_task2file_edge(dot, task, file)
            dot.attr(rankdir='TB')
            dot.engine = 'dot'
            dot.render(svg_file_path_without_suffix, format='svg', view=False)

        data['subgraph_id_list'] = list(template_manager.subgraphs.keys())
        data['subgraph_num_tasks_list'] = [len(subgraph) for subgraph in template_manager.subgraphs.values()]

        data['subgraph_id'] = subgraph_id
        data['subgraph_num_tasks'] = len(subgraph)
        data['subgraph_svg_content'] = open(svg_file_path, 'r').read()

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
