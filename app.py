from flask import Flask, render_template, jsonify, Response, request, send_from_directory
import os
import argparse
import ast
import pandas as pd
from typing import Dict, Any
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

    def restore_from_checkpoint(self):
        self.manager, self.workers, self.files, self.tasks = self.data_parser.restore_from_checkpoint()

    def change_runtime_template(self, runtime_template):
        runtime_template = runtime_template.split('/')[-1]
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

        if not self.runtime_template or runtime_template.split('/')[-1] != self.runtime_template.split('/')[-1]:
            self.change_runtime_template(runtime_template)


app = Flask(__name__)

@app.route('/api/execution-details')
def get_execution_details():
    try:
        # Get the runtime_template from query parameters or use the most recent one
        runtime_template = request.args.get('runtime_template')
        global_state.ensure_runtime_template(runtime_template)
        
        data: Dict[str, Any] = {}

        data['xMin'] = 0
        data['xMax'] = global_state.MAX_TIME - global_state.MIN_TIME

        # tasks information
        data['doneTasks'] = []
        data['failedTasks'] = []
        for task in global_state.tasks.values():
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
                    'when_ready': task.when_ready - global_state.MIN_TIME,
                    'when_running': task.when_running - global_state.MIN_TIME,
                    'time_worker_start': task.time_worker_start - global_state.MIN_TIME,
                    'time_worker_end': task.time_worker_end - global_state.MIN_TIME,
                    'when_waiting_retrieval': task.when_waiting_retrieval - global_state.MIN_TIME,
                    'when_retrieved': task.when_retrieved - global_state.MIN_TIME,
                    'when_done': task.when_done - global_state.MIN_TIME, 
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
                    'when_ready': task.when_ready - global_state.MIN_TIME,
                    'when_running': task.when_running - global_state.MIN_TIME,
                    'when_failure_happens': task.when_failure_happens - global_state.MIN_TIME,
                }
                data['failedTasks'].append(failed_task_info)

        data['workerInfo'] = []
        for worker in global_state.workers.values():
            if not worker.hash:
                continue
            worker_info = {
                'hash': worker.hash,
                'id': worker.id,
                'time_connected': [t - global_state.MIN_TIME for t in worker.time_connected],
                'time_disconnected': [t - global_state.MIN_TIME for t in worker.time_disconnected],
                'cores': worker.cores,
            }
            data['workerInfo'].append(worker_info)

        return jsonify(data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

@app.route('/api/storage-consumption')
def get_storage_consumption():
    try:
        # Get the runtime_template from query parameters or use the most recent one
        runtime_template = request.args.get('runtime_template')
        global_state.ensure_runtime_template(runtime_template)
        
        data = {}

        files = global_state.files

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

                data['worker_storage_consumption'][destination].append((float(transfer.time_stage_in) - global_state.MIN_TIME, file.size_mb))
                data['worker_storage_consumption'][destination].append((float(transfer.time_stage_out) - global_state.MIN_TIME, -file.size_mb))

        # sort the worker storage consumption
        for destination in data['worker_storage_consumption']:
            # convert to a pandas dataframe
            df = pd.DataFrame(data['worker_storage_consumption'][destination], columns=['time', 'size'])
            # sort the dataframe
            df = df.sort_values(by='time')
            # accumulate the size
            df['storage_consumption'] = df['size'].cumsum()
            # if some entries have the same time, only keep the last one
            df = df.drop_duplicates(subset='time', keep='last')
            # keep only time and storage_consumption columns
            data['worker_storage_consumption'][destination] = df[['time', 'storage_consumption']].values.tolist()
            # add the initial point at time_connected with 0 consumption
            for time_connected, time_disconnected in zip(global_state.workers[destination].time_connected, global_state.workers[destination].time_disconnected):
                data['worker_storage_consumption'][destination].insert(0, [time_connected - global_state.MIN_TIME, 0])
                data['worker_storage_consumption'][destination].append([time_disconnected - global_state.MIN_TIME, 0])
            
        # convert the key to a string
        data['worker_storage_consumption'] = {f"{k[0]}:{k[1]}": v for k, v in data['worker_storage_consumption'].items()}

        return jsonify(data)

    except Exception as e:
        print(f"Error in get_storage_consumption: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/logs/<runtime_template>')
def render_log_page(runtime_template):
    log_folders = [name for name in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    log_folders_sorted = sorted(log_folders)
    if runtime_template != global_state.runtime_template:
        global_state.change_runtime_template(runtime_template)
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

    global_state = TemplateState()
    
    app.run(host='0.0.0.0', port=args.port, debug=True)
