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
        runtime_template = Path(runtime_template).name
        runtime_template = os.path.join(os.getcwd(), LOGS_DIR, runtime_template)
        self.runtime_template = runtime_template
        self.data_parser = DataParser(runtime_template)
        self.restore_from_checkpoint()
        self.MIN_TIME = float(self.manager.time_start)
        self.MAX_TIME = float(self.manager.time_end)

    def ensure_runtime_template(self, runtime_template):
        # Get the runtime_template from query parameters or use the most recent one
        if not runtime_template:
            all_templates = sorted([d for d in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, d))])
            if not all_templates:
                return jsonify({'error': 'No runtime templates found'}), 404
            runtime_template = all_templates[-1]

        if not self.runtime_template or Path(runtime_template).name != Path(self.runtime_template).name:
            self.change_runtime_template(runtime_template)


app = Flask(__name__)

@app.route('/api/execution-details')
def get_execution_details():
    try:
        # Get the runtime_template from query parameters or use the most recent one
        runtime_template = request.args.get('runtime_template')
        template_manager.ensure_runtime_template(runtime_template)
        
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
        # Get the runtime_template from query parameters or use the most recent one
        runtime_template = request.args.get('runtime_template')
        template_manager.ensure_runtime_template(runtime_template)
        
        data = {}

        files = template_manager.files

        # construct the succeeded file transfers
        data['worker_storage_consumption'] = {}
        for file in files.values():
            for transfer in file.transfers.values():
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
        # Get the runtime_template from query parameters or use the most recent one
        runtime_template = request.args.get('runtime_template')
        template_manager.ensure_runtime_template(runtime_template)
        
        data = {}

        successful_transfers = 0
        failed_transfers = 0
        # construct the worker transfers
        data['incoming_transfers'] = defaultdict(list)     # for destinations
        data['outgoing_transfers'] = defaultdict(list)     # for sources
        for file in template_manager.files.values():
            for transfer in file.transfers.values():
                source = transfer.source
                destination = transfer.destination
                
                # if the source is a worker
                if isinstance(source, tuple):
                    data['incoming_transfers'][destination].append((transfer.time_start_stage_in - template_manager.MIN_TIME, 1))
                    if transfer.time_stage_in:
                        data['incoming_transfers'][destination].append((transfer.time_stage_in - template_manager.MIN_TIME, -1))
                    elif transfer.time_stage_out:
                        data['incoming_transfers'][destination].append((transfer.time_stage_out - template_manager.MIN_TIME, -1))
                # if the destination is a worker
                if isinstance(destination, tuple):
                    data['outgoing_transfers'][source].append((transfer.time_start_stage_in - template_manager.MIN_TIME, 1))
                    if transfer.time_stage_in:
                        data['outgoing_transfers'][source].append((transfer.time_stage_in - template_manager.MIN_TIME, -1))
                    elif transfer.time_stage_out:
                        data['outgoing_transfers'][source].append((transfer.time_stage_out - template_manager.MIN_TIME, -1))

        for destination in data['incoming_transfers']:
            df = pd.DataFrame(data['incoming_transfers'][destination], columns=['time', 'event'])
            df = df.sort_values(by=['time'])
            df['cumulative_event'] = df['event'].cumsum()
            data['incoming_transfers'][destination] = df[['time', 'cumulative_event']].values.tolist()
        for source in data['outgoing_transfers']:
            df = pd.DataFrame(data['outgoing_transfers'][source], columns=['time', 'event'])
            df = df.sort_values(by=['time'])
            df['cumulative_event'] = df['event'].cumsum()
            data['outgoing_transfers'][source] = df[['time', 'cumulative_event']].values.tolist()

        # convert keys to string-formatted keys
        data['incoming_transfers'] = {f"{k[0]}:{k[1]}": v for k, v in data['incoming_transfers'].items()}
        data['outgoing_transfers'] = {f"{k[0]}:{k[1]}": v for k, v in data['outgoing_transfers'].items()}
        
        return jsonify(data)

    except Exception as e:
        print(f"Error in get_peer2peer_transfer: {str(e)}")
        return jsonify({'error': str(e)}), 500



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

@app.route('/data/<path:filename>')
def serve_from_data(filename):
    return send_from_directory('data', filename)

@app.route('/logs/<path:filename>')
def serve_file(filename):
    base_directory = os.path.abspath("logs/")
    file_path = os.path.join(base_directory, filename)

    # stream the file
    if not os.path.exists(file_path):
        # skip and don't abort
        return Response(status=404)
    def generate():
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(4096)
                if not chunk:
                    break
                yield chunk

    return Response(generate(), mimetype='text/plain')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default=9122, help='Port number')
    args = parser.parse_args()

    template_manager = TemplateState()
    
    app.run(host='0.0.0.0', port=args.port, debug=True)
