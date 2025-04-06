from flask import Flask, render_template, jsonify, Response, request, send_from_directory
import os
import argparse
import pandas as pd
from typing import Dict, Any
from pathlib import Path
from collections import defaultdict
import graphviz
from src.data_parse import DataParser
import numpy as np
import random
import traceback
import time

LOGS_DIR = 'logs'
TARGET_POINTS = 10000  # at lease 3: the beginning, the end, and the global peak
TARGET_TASK_BARS = 100000   # how many task bars to show

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

    def change_runtime_template(self, runtime_template):
        if not runtime_template:
            return
        if self.runtime_template and Path(runtime_template).name == Path(self.runtime_template).name:
            print(f"Runtime template already set to: {runtime_template}")
            return
        self.runtime_template = os.path.join(os.getcwd(), LOGS_DIR, Path(runtime_template).name)
        print(f"Restoring data for: {runtime_template}")

        self.data_parser = DataParser(self.runtime_template)
        self.svg_files_dir = self.data_parser.svg_files_dir

        self.data_parser.restore_from_checkpoint()
        self.manager = self.data_parser.manager
        self.workers = self.data_parser.workers
        self.files = self.data_parser.files
        self.tasks = self.data_parser.tasks
        self.subgraphs = self.data_parser.subgraphs

        self.MIN_TIME = self.manager.when_first_task_start_commit
        self.MAX_TIME = self.manager.time_end

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
        data['num_of_status'] = defaultdict(int)
        data['num_successful_recovery_tasks'] = 0
        data['num_unsuccessful_recovery_tasks'] = 0
        for task in template_manager.tasks.values():
            if task.task_status == 0:
                # note that the task might have not been retrieved yet
                if not task.when_retrieved:
                    continue
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
                    'when_ready': task.when_ready - template_manager.MIN_TIME,
                    'when_running': task.when_running - template_manager.MIN_TIME,
                    'time_worker_start': task.time_worker_start - template_manager.MIN_TIME,
                    'time_worker_end': task.time_worker_end - template_manager.MIN_TIME,
                    'execution_time': task.time_worker_end - task.time_worker_start,
                    'when_waiting_retrieval': task.when_waiting_retrieval - template_manager.MIN_TIME,
                    'when_retrieved': task.when_retrieved - template_manager.MIN_TIME,
                }
                data['successfulTasks'].append(done_task_info)
            else:
                if len(task.core_id) == 0:    # not run at all
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
                    'when_ready': task.when_ready - template_manager.MIN_TIME,
                    'when_running': task.when_running - template_manager.MIN_TIME,
                    'when_failure_happens': task.when_failure_happens - template_manager.MIN_TIME,
                    'execution_time': task.when_failure_happens - task.when_running,
                }
                data['unsuccessfulTasks'].append(unsuccessful_task_info)

        # filter successfulTasks to keep only top 100,000 by execution time if there are more than 100,000 tasks
        if len(data['successfulTasks']) > TARGET_TASK_BARS:
            # sort tasks by execution time in descending order and keep top 100,000
            data['successfulTasks'] = sorted(data['successfulTasks'], 
                                          key=lambda x: x['execution_time'],
                                          reverse=True)[:TARGET_TASK_BARS]
        # filter unsuccessfulTasks to keep only top 100,000 by execution time if there are more than 100,000 tasks
        if len(data['unsuccessfulTasks']) > TARGET_TASK_BARS:
            # sort tasks by execution time in descending order and keep top 100,000
            data['unsuccessfulTasks'] = sorted(data['unsuccessfulTasks'], 
                                          key=lambda x: x['execution_time'],
                                          reverse=True)[:TARGET_TASK_BARS]
        
        data['workerInfo'] = []
        for worker in template_manager.workers.values():
            if not worker.hash:
                continue
            # it means the worker didn't exit normally or hasn't exited yet
            if len(worker.time_disconnected) != len(worker.time_connected):
                # set the time_disconnected to the max time
                worker.time_disconnected = [template_manager.MAX_TIME] * (len(worker.time_connected) - len(worker.time_disconnected))
            worker_info = {
                'hash': worker.hash,
                'id': worker.id,
                'worker_ip_port': f"{worker.ip}:{worker.port}",
                'time_connected': [max(t - template_manager.MIN_TIME, 0) for t in worker.time_connected],
                'time_disconnected': [max(t - template_manager.MIN_TIME, 0) for t in worker.time_disconnected],
                'cores': worker.cores,
                'memory_mb': worker.memory_mb,
                'disk_mb': worker.disk_mb,
                'gpus': worker.gpus,
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
        
        # Calculate yTickValues for worker IDs
        worker_ids = [worker['id'] for worker in data['workerInfo']]
        
        if worker_ids:
            min_worker_id = 1  # Start with worker ID 1
            max_worker_id = max(worker_ids)
            
            print(f"Min worker ID: {min_worker_id}, Max worker ID: {max_worker_id}")
            
            # Generate 5 evenly distributed tick values
            if min_worker_id == max_worker_id:
                data['yTickValues'] = [min_worker_id]
            else:
                step = (max_worker_id - min_worker_id) / 4  # To get 5 points total
                data['yTickValues'] = [
                    min_worker_id,
                    round(min_worker_id + step, 0),
                    round(min_worker_id + 2 * step, 0),
                    round(min_worker_id + 3 * step, 0),
                    max_worker_id
                ]
                # Convert to integers
                data['yTickValues'] = [int(tick) for tick in data['yTickValues']]
                # Remove duplicates while preserving order
                data['yTickValues'] = list(dict.fromkeys(data['yTickValues']))
                
            print(f"Generated yTickValues: {data['yTickValues']}")
        else:
            data['yTickValues'] = [1]  # Default if no workers
            print("No workers, using default yTickValues: [1]")

        return jsonify(data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

def downsample_storage_data(points):
    # downsample storage consumption data points while keeping the global peak and randomly sampling other points
    if len(points) <= TARGET_POINTS:
        return points

    global_peak_idx = max(range(len(points)), key=lambda i: points[i][1])
    global_peak = points[global_peak_idx]

    keep_indices = {0, len(points) - 1, global_peak_idx}

    remaining_points = TARGET_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, points[-1]]

    available_indices = list(set(range(len(points))) - keep_indices)
    sampled_indices = random.sample(available_indices, min(remaining_points, len(available_indices)))
    keep_indices.update(sampled_indices)
    
    result = [points[i] for i in sorted(keep_indices)]
    return result

@app.route('/api/storage-consumption')
def get_storage_consumption():
    try:
        show_percentage = request.args.get('show_percentage', 'false').lower() == 'true'
        show_pbb_workers = request.args.get('show_pbb_workers', 'true').lower() == 'true'
        data = {}

        files = template_manager.files

        # construct the succeeded file transfers
        data['worker_storage_consumption'] = {}
        data['worker_resources'] = {}
        
        all_worker_storage = {}
        
        for file in files.values():
            for transfer in file.transfers:
                # skip if this is not a transfer to a worker
                if not isinstance(transfer.destination, tuple):
                    continue
                # skip if the transfer was not successful
                if transfer.time_stage_in is None:
                    continue

                destination = transfer.destination

                # skip if this is a PBB worker
                if destination in template_manager.workers:
                    if template_manager.workers[destination].is_pbb:
                        if not show_pbb_workers:
                            continue

                # add the transfer to the worker
                if destination not in all_worker_storage:
                    all_worker_storage[destination] = []

                # time_in = float(transfer.time_stage_in - template_manager.MIN_TIME)
                time_in = float(transfer.time_start_stage_in - template_manager.MIN_TIME)
                time_out = float(transfer.time_stage_out - template_manager.MIN_TIME)
                
                # Skip if times are invalid
                #if np.isnan(time_in):
                    #continue
                    
                all_worker_storage[destination].append((time_in, max(0, file.size_mb)))
                all_worker_storage[destination].append((time_out, -max(0, file.size_mb)))

        for destination in list(all_worker_storage.keys()):          
            data['worker_storage_consumption'][destination] = all_worker_storage[destination]

        max_storage_consumption = 0
        for destination in list(data['worker_storage_consumption'].keys()):
            if not data['worker_storage_consumption'][destination]:
                del data['worker_storage_consumption'][destination]
                continue
                
            # convert to a pandas dataframe
            df = pd.DataFrame(data['worker_storage_consumption'][destination], columns=['time', 'size'])
            # sort the dataframe by time
            df = df.sort_values(by=['time'])
            # accumulate the size
            df['storage_consumption'] = df['size'].cumsum()
            df['storage_consumption'] = df['storage_consumption'].clip(lower=0)  # Ensure no negative values
            
            if show_percentage:
                # Use worker's total disk space from worker.disk_mb
                worker_disk_mb = template_manager.workers[destination].disk_mb
                if worker_disk_mb > 0:  # Avoid division by zero
                    df['storage_consumption'] = (df['storage_consumption'] / worker_disk_mb) * 100
            
            # group by time and keep the maximum storage consumption for each time
            df = df.groupby('time')['storage_consumption'].max().reset_index()
            
            # Skip if no valid data
            if df.empty or df['storage_consumption'].isna().all():
                del data['worker_storage_consumption'][destination]
                continue
                
            # update the max storage consumption
            curr_max = df['storage_consumption'].max()
            if not np.isnan(curr_max):
                max_storage_consumption = max(max_storage_consumption, curr_max)
                
            # Convert to list of points and downsample
            points = df[['time', 'storage_consumption']].values.tolist()
            points = downsample_storage_data(points)
            data['worker_storage_consumption'][destination] = points
            
            # add the initial and final points with 0 consumption
            worker = template_manager.workers[destination]
            if worker.time_connected and worker.time_disconnected:
                for time_connected, time_disconnected in zip(worker.time_connected, worker.time_disconnected):
                    time_start = float(time_connected - template_manager.MIN_TIME)
                    time_end = float(time_disconnected - template_manager.MIN_TIME)
                    if not np.isnan(time_start):
                        data['worker_storage_consumption'][destination].insert(0, [time_start, 0.0])
                    if not np.isnan(time_end):
                        data['worker_storage_consumption'][destination].append([time_end, 0.0])

        # Skip if no valid data for any worker
        if not data['worker_storage_consumption']:
            return jsonify({'error': 'No valid storage consumption data available'}), 404

        # convert the key to a string
        data['worker_storage_consumption'] = {f"{k[0]}:{k[1]}": v for k, v in data['worker_storage_consumption'].items()}

        if show_percentage:
            data['file_size_unit'] = '%'
            max_storage_consumption = 100
        else:
            data['file_size_unit'], scale = get_unit_and_scale_by_max_file_size_mb(max_storage_consumption)
            # also update the source data
            if scale != 1:
                for destination in data['worker_storage_consumption']:
                    points = data['worker_storage_consumption'][destination]
                    data['worker_storage_consumption'][destination] = [[p[0], p[1] * scale] for p in points]
                max_storage_consumption *= scale

        # Add worker resource information
        for worker_entry, worker in template_manager.workers.items():
            worker_id = f"{worker_entry[0]}:{worker_entry[1]}"
            if worker_id in data['worker_storage_consumption']:
                data['worker_resources'][worker_id] = {
                    'cores': worker.cores,
                    'memory_mb': worker.memory_mb,
                    'disk_mb': worker.disk_mb,
                    'gpus': worker.gpus,
                    'is_pbb': getattr(worker, 'is_pbb', False)
                }

        data['show_pbb_workers'] = show_pbb_workers

        # plotting parameters
        data['xMin'] = 0
        data['xMax'] = float(template_manager.MAX_TIME - template_manager.MIN_TIME)
        data['yMin'] = 0
        data['yMax'] = max(1.0, max_storage_consumption)  # Ensure positive yMax and at least 1.0
        
        # Ensure all tick values are valid numbers
        x_range = data['xMax'] - data['xMin']
        data['xTickValues'] = [
            float(data['xMin']),
            float(data['xMin'] + x_range * 0.25),
            float(data['xMin'] + x_range * 0.5),
            float(data['xMin'] + x_range * 0.75),
            float(data['xMax'])
        ]
        
        y_range = data['yMax'] - data['yMin']
        data['yTickValues'] = [
            float(data['yMin']),
            float(data['yMin'] + y_range * 0.25),
            float(data['yMin'] + y_range * 0.5),
            float(data['yMin'] + y_range * 0.75),
            float(data['yMax'])
        ]
        
        data['tickFontSize'] = int(template_manager.tick_size)

        return jsonify(data)

    except Exception as e:
        print(f"Error in get_storage_consumption: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    

def downsample_worker_transfers(points):
    # downsample transfer data points while keeping the global peak and randomly sampling other points
    if len(points) <= TARGET_POINTS:
        return points

    global_peak_idx = max(range(len(points)), key=lambda i: points[i][1])
    global_peak = points[global_peak_idx]

    keep_indices = {0, len(points) - 1, global_peak_idx}

    remaining_points = TARGET_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, points[-1]]

    available_indices = list(set(range(len(points))) - keep_indices)
    sampled_indices = random.sample(available_indices, min(remaining_points, len(available_indices)))
    keep_indices.update(sampled_indices)

    result = [points[i] for i in sorted(keep_indices)]
    return result

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
            
            # Convert to list of points and downsample
            points = df[['time', 'cumulative_transfers']].values.tolist()
            points = downsample_worker_transfers(points)
            data['transfers'][worker] = points
            
            # append the initial point at time_connected with 0
            for time_connected, time_disconnected in zip(template_manager.workers[worker].time_connected, template_manager.workers[worker].time_disconnected):
                data['transfers'][worker].insert(0, [time_connected - template_manager.MIN_TIME, 0])
                data['transfers'][worker].append([time_disconnected - template_manager.MIN_TIME, 0])
            max_transfers = max(max_transfers, max(point[1] for point in points))

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

def downsample_task_execution_time(points):
    # downsample task execution time points while keeping the first point, last point, and peak execution time
    if len(points) <= TARGET_POINTS:
        return points

    # Find global peak (maximum execution time)
    global_peak_idx = max(range(len(points)), key=lambda i: points[i][1])  # points[i][1] is execution_time
    global_peak = points[global_peak_idx]

    # Keep the first point, last point, and global peak
    keep_indices = {0, len(points) - 1, global_peak_idx}

    # Calculate remaining points to sample
    remaining_points = TARGET_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, points[-1]]

    # Sort the indices we want to keep to find gaps between them
    sorted_keep_indices = sorted(keep_indices)
    
    # Calculate points to keep in each gap
    points_per_gap = remaining_points // (len(sorted_keep_indices) - 1)
    extra_points = remaining_points % (len(sorted_keep_indices) - 1)

    # For each gap between key points, randomly sample points
    for i in range(len(sorted_keep_indices) - 1):
        start_idx = sorted_keep_indices[i]
        end_idx = sorted_keep_indices[i + 1]
        gap_size = end_idx - start_idx - 1
        
        if gap_size <= 0:
            continue
            
        # Calculate how many points to keep in this gap
        current_gap_points = points_per_gap
        if extra_points > 0:
            current_gap_points += 1
            extra_points -= 1
            
        if current_gap_points > 0:
            # Randomly sample points from this gap
            available_indices = list(range(start_idx + 1, end_idx))
            sampled_indices = random.sample(available_indices, min(current_gap_points, len(available_indices)))
            keep_indices.update(sampled_indices)
    
    # Sort all indices and return the corresponding points
    result = [points[i] for i in sorted(keep_indices)]
    return result

@app.route('/api/task-execution-time')
def get_task_execution_time():
    try:
        data = {}

        # get the execution time of each task
        task_execution_time_list = []
        for task in template_manager.tasks.values():
            # skip if the task didn't run to completion
            if task.task_status != 0:
                continue
            task_execution_time = round(task.time_worker_end - task.time_worker_start, 2)
            # if a task completes very quickly, we set it to 0.01
            task_execution_time = max(task_execution_time, 0.01)
            task_execution_time_list.append((task.task_id, task_execution_time))

        # sort by execution time for better visualization
        task_execution_time_list.sort(key=lambda x: x[1])
        
        # downsample the data points
        # data['task_execution_time'] = downsample_task_execution_time(task_execution_time_list)
        data['task_execution_time'] = task_execution_time_list

        # calculate the cdf using the original (non-downsampled) data to maintain accuracy
        df = pd.DataFrame(task_execution_time_list, columns=['task_id', 'task_execution_time'])
        df['cumulative_execution_time'] = df['task_execution_time'].cumsum()
        df['probability'] = df['cumulative_execution_time'] / df['cumulative_execution_time'].max()
        df['probability'] = df['probability'].round(4)
        
        # downsample the CDF data points as well
        cdf_points = df[['task_execution_time', 'probability']].values.tolist()
        # data['task_execution_time_cdf'] = downsample_task_execution_time(cdf_points)
        data['task_execution_time_cdf'] = cdf_points

        # tick values - use original data ranges to maintain proper axis scaling
        num_tasks = len(task_execution_time_list)  # use original length
        data['execution_time_x_tick_values'] = [
            1,
            round(num_tasks * 0.25, 2),
            round(num_tasks * 0.5, 2),
            round(num_tasks * 0.75, 2),
            num_tasks
        ]
        
        max_execution_time = max(x[1] for x in task_execution_time_list)  # use original max
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
        print(f"Error in get_task_execution_time: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
def downsample_task_concurrency(points):
    # If points are fewer than target, return all
    if len(points) <= TARGET_POINTS:
        return points

    # Find global peak (maximum concurrency)
    global_peak_idx = max(range(len(points)), key=lambda i: points[i][1])  # points[i][1] is concurrency
    global_peak = points[global_peak_idx]

    # Keep first, last and peak points
    keep_indices = {0, len(points) - 1, global_peak_idx}

    # Calculate remaining points to sample
    remaining_points = TARGET_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, points[-1]]

    # Sort key indices to find gaps
    sorted_keep_indices = sorted(keep_indices)
    
    # Calculate points per gap
    points_per_gap = remaining_points // (len(sorted_keep_indices) - 1)
    extra_points = remaining_points % (len(sorted_keep_indices) - 1)

    # Sample points from each gap
    for i in range(len(sorted_keep_indices) - 1):
        start_idx = sorted_keep_indices[i]
        end_idx = sorted_keep_indices[i + 1]
        gap_size = end_idx - start_idx - 1
        
        if gap_size <= 0:
            continue
            
        # Calculate points for this gap
        current_gap_points = points_per_gap
        if extra_points > 0:
            current_gap_points += 1
            extra_points -= 1
            
        if current_gap_points > 0:
            # Randomly sample from gap
            available_indices = list(range(start_idx + 1, end_idx))
            sampled_indices = random.sample(available_indices, min(current_gap_points, len(available_indices)))
            keep_indices.update(sampled_indices)
    
    # Return sorted points
    result = [points[i] for i in sorted(keep_indices)]
    return result

@app.route('/api/task-concurrency')
def get_task_concurrency():
    try:
        data = {}
        
        # Get selected task types
        selected_types = request.args.get('types', '').split(',')
        if not selected_types or selected_types == ['']:
            selected_types = [
                'tasks_waiting',
                'tasks_committing',
                'tasks_executing',
                'tasks_retrieving',
                'tasks_done'
            ]
        
        # Initialize task type lists
        all_task_types = {
            'tasks_waiting': [],
            'tasks_committing': [],
            'tasks_executing': [],
            'tasks_retrieving': [],
            'tasks_done': []
        }
        data.update(all_task_types)
        
        # Process selected task types
        for task in template_manager.tasks.values():
            if task.when_failure_happens is not None:
                continue
                
            # Collect task state data
            if 'tasks_waiting' in selected_types and task.when_ready:
                data['tasks_waiting'].append((task.when_ready - template_manager.MIN_TIME, 1))
                if task.when_running:
                    data['tasks_waiting'].append((task.when_running - template_manager.MIN_TIME, -1))
            
            if 'tasks_committing' in selected_types and task.when_running:
                data['tasks_committing'].append((task.when_running - template_manager.MIN_TIME, 1))
                if task.time_worker_start:
                    data['tasks_committing'].append((task.time_worker_start - template_manager.MIN_TIME, -1))
            
            if 'tasks_executing' in selected_types and task.time_worker_start:
                data['tasks_executing'].append((task.time_worker_start - template_manager.MIN_TIME, 1))
                if task.time_worker_end:
                    data['tasks_executing'].append((task.time_worker_end - template_manager.MIN_TIME, -1))
            
            if 'tasks_retrieving' in selected_types and task.time_worker_end:
                data['tasks_retrieving'].append((task.time_worker_end - template_manager.MIN_TIME, 1))
                if task.when_retrieved:
                    data['tasks_retrieving'].append((task.when_retrieved - template_manager.MIN_TIME, -1))
            
            if 'tasks_done' in selected_types:
                if task.when_done:
                    data['tasks_done'].append((task.when_done - template_manager.MIN_TIME, 1))

        def process_task_type(tasks):
            if not tasks:
                return []
            # Convert to DataFrame and calculate cumulative events
            df = pd.DataFrame(tasks, columns=['time', 'event'])
            df = df.sort_values(by=['time'])
            df['time'] = df['time'].round(2)
            df['cumulative_event'] = df['event'].cumsum()
            # Keep last event for duplicate timestamps
            df = df.drop_duplicates(subset=['time'], keep='last')
            points = df[['time', 'cumulative_event']].values.tolist()
            # Downsample data
            return downsample_task_concurrency(points)

        # Process all task types data
        max_time = float('-inf')
        max_concurrent = 0
        for task_type in all_task_types:
            data[task_type] = process_task_type(data[task_type])
            # Update max values
            if data[task_type]:
                max_time = max(max_time, max(point[0] for point in data[task_type]))
                if task_type in selected_types:
                    max_concurrent = max(max_concurrent, max(point[1] for point in data[task_type]))

        # Set axis ranges
        data['xMin'] = 0
        data['xMax'] = max_time
        data['yMin'] = 0
        data['yMax'] = max_concurrent

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
        print(f"Error in get_task_concurrency: {str(e)}")
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
    
def downsample_file_replicas(points):
    # downsample file replicas data points while keeping the global peak and randomly sampling other points
    if len(points) <= TARGET_POINTS:
        return points

    # Find global peak (maximum number of replicas)
    global_peak_idx = max(range(len(points)), key=lambda i: points[i][3])  # points[i][3] is num_replicas
    global_peak = points[global_peak_idx]

    # Keep the first point, last point, and global peak
    keep_indices = {0, len(points) - 1, global_peak_idx}

    # Calculate how many points we need to keep between each key point
    remaining_points = TARGET_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, points[-1]]

    # Sort the indices we want to keep to find gaps between them
    sorted_keep_indices = sorted(keep_indices)
    
    # Calculate points to keep in each gap
    points_per_gap = remaining_points // (len(sorted_keep_indices) - 1)
    extra_points = remaining_points % (len(sorted_keep_indices) - 1)

    # For each gap between key points, randomly sample points
    for i in range(len(sorted_keep_indices) - 1):
        start_idx = sorted_keep_indices[i]
        end_idx = sorted_keep_indices[i + 1]
        gap_size = end_idx - start_idx - 1
        
        if gap_size <= 0:
            continue
            
        # Calculate how many points to keep in this gap
        current_gap_points = points_per_gap
        if extra_points > 0:
            current_gap_points += 1
            extra_points -= 1
            
        if current_gap_points > 0:
            # Randomly sample points from this gap
            available_indices = list(range(start_idx + 1, end_idx))
            sampled_indices = random.sample(available_indices, min(current_gap_points, len(available_indices)))
            keep_indices.update(sampled_indices)
    
    # Sort all indices and return the corresponding points
    result = [points[i] for i in sorted(keep_indices)]
    return result

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

        # convert numpy int to python int
        df['num_replicas'] = df['num_replicas'].astype(int)
        df['file_size'] = df['file_size'].astype(int)

        # Convert to list of points and downsample
        points = df.values.tolist()
        points = downsample_file_replicas(points)
        data['file_replicas'] = points
        
        # ploting parameters
        if len(points) == 0:
            data['xMin'] = 1
            data['xMax'] = 1
            data['yMin'] = 0    
            data['yMax'] = 0
        else:
            data['xMin'] = 1
            data['xMax'] = len(df)  # Use original length for x-axis
            data['yMin'] = 0    
            data['yMax'] = int(df['num_replicas'].max())  # Use original max for y-axis
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
    
def downsample_file_sizes(points):
    # downsample file sizes data points while keeping the global peak and randomly sampling other points
    if len(points) <= TARGET_POINTS:
        return points

    # Find global peak (maximum file size)
    global_peak_idx = max(range(len(points)), key=lambda i: points[i][2])  # points[i][2] is file_size
    global_peak = points[global_peak_idx]

    # Find x-axis maximum (latest file)
    x_max_idx = max(range(len(points)), key=lambda i: points[i][3])  # points[i][3] is file_created_time
    x_max_point = points[x_max_idx]

    # Keep the first point, last point, global peak, and x-axis maximum
    keep_indices = {0, len(points) - 1, global_peak_idx, x_max_idx}

    # Calculate how many points we need to keep between each key point
    remaining_points = TARGET_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, x_max_point, points[-1]]

    # Sort the indices we want to keep to find gaps between them
    sorted_keep_indices = sorted(keep_indices)
    
    # Calculate points to keep in each gap
    points_per_gap = remaining_points // (len(sorted_keep_indices) - 1)
    extra_points = remaining_points % (len(sorted_keep_indices) - 1)

    # For each gap between key points, randomly sample points
    for i in range(len(sorted_keep_indices) - 1):
        start_idx = sorted_keep_indices[i]
        end_idx = sorted_keep_indices[i + 1]
        gap_size = end_idx - start_idx - 1
        
        if gap_size <= 0:
            continue
            
        # Calculate how many points to keep in this gap
        current_gap_points = points_per_gap
        if extra_points > 0:
            current_gap_points += 1
            extra_points -= 1
            
        if current_gap_points > 0:
            # Randomly sample points from this gap
            available_indices = list(range(start_idx + 1, end_idx))
            sampled_indices = random.sample(available_indices, min(current_gap_points, len(available_indices)))
            keep_indices.update(sampled_indices)
    
    # Sort all indices and return the corresponding points
    result = [points[i] for i in sorted(keep_indices)]
    return result

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
        
        # Convert to list of points and downsample
        points = df.values.tolist()
        points = downsample_file_sizes(points)
        data['file_sizes'] = points

        # ploting parameters
        if len(points) == 0:
            data['xMin'] = 1
            data['xMax'] = 1
            data['yMin'] = 0    
            data['yMax'] = 0
        else:
            data['xMin'] = 1
            data['xMax'] = len(df)  # Use original length for x-axis
            data['yMin'] = 0    
            data['yMax'] = max_file_size_mb * scale  # Use original max for y-axis
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
        print(f"subgraph: {subgraph_id} has {len(subgraph)} tasks")

        svg_file_path_without_suffix = os.path.join(template_manager.svg_files_dir, f'subgraph-{subgraph_id}-{plot_unsuccessful_task}-{plot_recovery_task}')
        svg_file_path = f'{svg_file_path_without_suffix}.svg'

        if not Path(svg_file_path).exists():
            dot = graphviz.Digraph()
            
            # Use sets to cache added file nodes and edges to avoid duplication
            added_file_nodes = set()
            added_edges = set()
            
            num_of_task_nodes = 0
            num_of_file_nodes = 0
            num_of_edges = 0
            
            # Preprocess to analyze the execution history of each task_id
            task_stats = {}  # {task_id: {"failures": count, "attempts": []}}
            task_execution_order = {}  # {task_id: [ordered list of task_try_ids]}
            
            # First build a chronological order of attempts for each task
            for (task_id, task_try_id) in list(subgraph):
                task = template_manager.tasks[(task_id, task_try_id)]
                
                if task_id not in task_execution_order:
                    task_execution_order[task_id] = []
                
                # Add to the execution order list (we'll sort it later)
                task_execution_order[task_id].append({
                    "try_id": task_try_id,
                    "time": task.time_worker_start or 0,  # Use start time for ordering
                    "success": not task.when_failure_happens,
                    "is_recovery": task.is_recovery_task
                })
            
            # Sort attempts by time for each task
            for task_id, attempts in task_execution_order.items():
                task_execution_order[task_id] = sorted(attempts, key=lambda x: x["time"])
            
            # Now compute statistics based on the ordered execution history
            for task_id, attempts in task_execution_order.items():
                failures = 0
                latest_successful_try_id = None
                final_status_is_success = False
                is_recovery_task = False
                
                # Go through attempts in chronological order
                for attempt in attempts:
                    if not attempt["success"]:
                        failures += 1
                    else:
                        latest_successful_try_id = attempt["try_id"]
                        final_status_is_success = True
                    
                    # Check if this is a recovery task
                    if attempt["is_recovery"]:
                        is_recovery_task = True
                
                # Get the final attempt (chronologically last)
                final_attempt = attempts[-1] if attempts else None
                
                task_stats[task_id] = {
                    "failures": failures,
                    "latest_successful_try_id": latest_successful_try_id,
                    "final_status_is_success": final_status_is_success,
                    "is_recovery_task": is_recovery_task,
                    "final_attempt": final_attempt,
                    "attempts": attempts
                }
            
            def plot_task_node(dot, task):
                task_id = task.task_id
                task_try_id = task.task_try_id
                stats = task_stats[task_id]
                
                # Use a single node ID based on task_id
                node_id = f'{task_id}'
                
                # Create a detailed label 
                node_label = f'{task_id}'
                
                # Add recovery task label if applicable
                if stats["is_recovery_task"]:
                    node_label += " (Recovery Task)"
                
                # Add failed count if applicable
                if stats["failures"] > 0:
                    node_label += f" (Failed: {stats['failures']})"
                
                # Add information about the number of attempts
                total_attempts = len(stats["attempts"])
                if total_attempts > 1:
                    node_label += f" (Attempts: {total_attempts})"
                
                # Style based on task type and final status
                if not stats["final_status_is_success"]:
                    # Task ultimately failed
                    style = 'dashed'
                    color = '#FF0000'  # Red
                    fontcolor = '#FF0000'
                    fillcolor = '#FFFFFF'
                elif stats["is_recovery_task"]:
                    # This is a recovery task that succeeded
                    style = 'filled'
                    color = '#000000'
                    fontcolor = '#000000'
                    fillcolor = '#FFC0CB'  # Light pink
                elif stats["failures"] > 0:
                    # Task had failures but succeeded without being a recovery task
                    style = 'filled'
                    color = '#000000'
                    fontcolor = '#000000'
                    fillcolor = '#FFFACD'  # Light yellow
                else:
                    # Normal successful task without failures
                    style = 'solid'
                    color = '#000000'
                    fontcolor = '#000000'
                    fillcolor = '#FFFFFF'
                
                dot.node(node_id, node_label, shape='ellipse', style=style, color=color, 
                         fontcolor=fontcolor, fillcolor=fillcolor)
                return True

            def plot_file_node(dot, file):
                # Skip files not produced by any task
                if len(file.producers) == 0:
                    return
                
                file_name = file.filename
                # Check if file node has already been added
                if file_name not in added_file_nodes:
                    dot.node(file_name, file_name, shape='box')
                    added_file_nodes.add(file_name)
                    nonlocal num_of_file_nodes
                    num_of_file_nodes += 1

            def plot_task2file_edge(dot, task, file):
                # Skip files not produced by any task
                if len(file.producers) == 0:
                    return
                # Skip unsuccessful tasks (cannot produce files)
                if task.when_failure_happens:
                    return
                    
                # Use only task_id (not try_id) to connect to the aggregated task node
                edge_id = (f'{task.task_id}', file.filename)
                
                # Check if edge has already been added
                if edge_id in added_edges:
                    return
                
                task_execution_time = task.time_worker_end - task.time_worker_start
                dot.edge(edge_id[0], edge_id[1], label=f'{task_execution_time:.2f}s')
                added_edges.add(edge_id)
                nonlocal num_of_edges
                num_of_edges += 1
            
            def plot_file2task_edge(dot, file, task):
                # Skip files not produced by any task
                if len(file.producers) == 0:
                    return
                    
                # Use only task_id (not try_id) to connect to the aggregated task node
                edge_id = (file.filename, f'{task.task_id}')
                
                # Check if edge has already been added
                if edge_id in added_edges:
                    return
                
                # Calculate file creation time
                file_creation_time = float('inf')
                for producer_task_id, producer_task_try_id in file.producers:
                    producer_task = template_manager.tasks[(producer_task_id, producer_task_try_id)]
                    if producer_task.time_worker_end:
                        file_creation_time = min(file_creation_time, producer_task.time_worker_end)
                file_creation_time = file_creation_time - template_manager.MIN_TIME

                dot.edge(edge_id[0], edge_id[1], label=f'{file_creation_time:.2f}s')
                added_edges.add(edge_id)
                nonlocal num_of_edges
                num_of_edges += 1

            # Process tasks for display (one node per task_id)
            processed_task_ids = set()
            
            for task_id, stats in task_stats.items():
                if task_id in processed_task_ids:
                    continue
                
                # For visualization, prefer to use:
                # 1. The latest successful attempt if there is one
                # 2. Otherwise, use the final attempt (which would be a failure)
                if stats["latest_successful_try_id"] is not None:
                    task_try_id = stats["latest_successful_try_id"]
                else:
                    # If no successful attempts, use the final attempt
                    task_try_id = stats["final_attempt"]["try_id"] if stats["final_attempt"] else None
                
                if task_try_id is None:
                    continue  # Skip if we can't determine which attempt to use
                
                task = template_manager.tasks[(task_id, task_try_id)]
                
                # Skip based on display options
                if task.is_recovery_task and not plot_recovery_task:
                    continue
                if task.when_failure_happens and not plot_unsuccessful_task:
                    continue
                
                # Plot the node for this task
                if plot_task_node(dot, task):
                    num_of_task_nodes += 1
                    processed_task_ids.add(task_id)
                    
                    # Process input files
                    for file_name in task.input_files:
                        file = template_manager.files[file_name]
                        plot_file_node(dot, file)
                        plot_file2task_edge(dot, file, task)
                    
                    # Process output files
                    # Only plot outputs if this was a successful attempt
                    if not task.when_failure_happens:
                        for file_name in task.output_files:
                            file = template_manager.files[file_name]
                            # skip files that haven't been created
                            if len(file.transfers) == 0:
                                continue
                            plot_file_node(dot, file)
                            plot_task2file_edge(dot, task, file)

            print(f"num of task nodes: {num_of_task_nodes}")
            print(f"num of file nodes: {num_of_file_nodes}")
            print(f"num of edges: {num_of_edges}")
            print(f"total nodes: {num_of_task_nodes + num_of_file_nodes}")
            
            dot.attr(rankdir='TB')
            dot.engine = 'dot'
            dot.render(svg_file_path_without_suffix, format='svg', view=False)
            
            import time
            print(f"rendering subgraph: {subgraph_id}")
            time_start = time.time()
            dot.render(svg_file_path_without_suffix, format='svg', view=False)
            time_end = time.time()
            print(f"rendering subgraph: {subgraph_id} done in {round(time_end - time_start, 4)} seconds")

        data['subgraph_id_list'] = list(template_manager.subgraphs.keys())
        data['subgraph_num_tasks_list'] = [len(subgraph) for subgraph in template_manager.subgraphs.values()]

        data['subgraph_id'] = subgraph_id
        data['subgraph_num_tasks'] = len(subgraph)
        data['subgraph_svg_content'] = open(svg_file_path, 'r').read()

        return jsonify(data)
    except Exception as e:
        print(f"Error in get_subgraphs: {str(e)}")
        traceback.print_exc()
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
    
    app.run(host='0.0.0.0', port=args.port, debug=True, use_reloader=False)
