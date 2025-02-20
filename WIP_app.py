from flask import Flask, render_template, jsonify, Response, request, send_from_directory
import os
import argparse
import ast
import pandas as pd
import sys
import subprocess
from typing import Dict, Any
import json
import requests

def safe_literal_eval(val):
    try:
        return ast.literal_eval(val)
    except (ValueError, SyntaxError):
        return []

app = Flask(__name__)

LOGS_DIR = 'logs'

@app.route('/api/execution-details')
def get_execution_details():
    try:
        # Get the runtime_template from query parameters or use the most recent one
        runtime_template = request.args.get('runtime_template')
        if not runtime_template:
            all_templates = sorted([d for d in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, d))])
            if not all_templates:
                return jsonify({'error': 'No runtime templates found'}), 404
            runtime_template = all_templates[-1]

        csv_dir = os.path.join(LOGS_DIR, runtime_template, 'csv-files')
        json_dir = os.path.join(LOGS_DIR, runtime_template, 'json-files')
        
        data: Dict[str, Any] = {}

        # Read task_info.csv for both successful and failed tasks
        task_info_path = os.path.join(csv_dir, 'task_info.csv')
        if os.path.exists(task_info_path):
            task_df = pd.read_csv(task_info_path)
            
            # Filter successful tasks
            done_tasks = task_df[task_df['task_status'] == 0].copy()
            data['doneTasks'] = done_tasks[[
                'task_id', 'worker_ip', 'worker_port', 'worker_id', 'core_id', 'is_recovery_task', 'task_status', 'category',
                'when_ready', 'when_running', 'time_worker_start', 'time_worker_end', 'when_waiting_retrieval', 'when_retrieved', 'when_done',
            ]].to_dict(orient='records')
            # assert that there is no None in the data
            assert not any(d.get(k) is None for d in data['doneTasks'] for k in d.keys())

            # Failed tasks (task_status != 0)
            failed_tasks = task_df[task_df['task_status'] != 0].copy()
            data['failedTasks'] = failed_tasks[[
                'task_id', 'worker_ip', 'worker_port', 'worker_id', 'core_id', 'is_recovery_task', 'task_status', 'category',
                'when_ready', 'when_running', 'when_failure_happens',
            ]].to_dict(orient='records')
            # convert the None to "None"
            data['failedTasks'] = [
                {k: "None" if v is None else v for k, v in d.items()}
                for d in data['failedTasks']
            ]
        else:
            print(f"{task_info_path} not found")

        # Read worker_info.csv
        worker_info_path = os.path.join(csv_dir, 'worker_info.csv')
        if os.path.exists(worker_info_path):
            worker_df = pd.read_csv(worker_info_path)
            # filter out workers that have NaN columns
            worker_df = worker_df[worker_df.notna().all(axis=1)]
            data['workerInfo'] = worker_df[[
                'hash', 'id', 'time_connected', 'time_disconnected', 'cores'
            ]].to_dict(orient='records')
            # assert that there is no None in the data
            assert not any(d.get(k) is None for d in data['workerInfo'] for k in d.keys())
        else:
            print(f"{worker_info_path} not found")

        # Read manager_info.json
        manager_info_path = os.path.join(json_dir, 'manager_info.json')
        if os.path.exists(manager_info_path):
            with open(manager_info_path, 'r') as f:
                data['managerInfo'] = json.load(f)
                # remove the keys where the value is None
                data['managerInfo'] = {k: v for k, v in data['managerInfo'].items() if v is not None}
        else:
            print(f"{manager_info_path} not found")

        return jsonify(data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/logs/<log_name>')
def render_log_page(log_name):
    log_folders = [name for name in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    log_folders_sorted = sorted(log_folders)
    return render_template('index.html', log_folders=log_folders_sorted, current_log=log_name)


@app.route('/')
def index():
    log_folders = [name for name in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    log_folders_sorted = sorted(log_folders)
    return render_template('index.html', log_folders=log_folders_sorted)

@app.route('/logs/<log_folder>')
def logs(log_folder):
    global CURRENT_LOG
    CURRENT_LOG = log_folder
    log_folder_path = os.path.join(LOGS_DIR, log_folder, 'vine-logs')
    if os.path.exists(log_folder_path) and os.path.isdir(log_folder_path):
        return jsonify({'logPath': log_folder_path})
    return jsonify({'error': 'Log folder not found'}), 404

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
    parser.add_argument('--runtime-template', help='Specific runtime template to query')
    parser.add_argument('--port', default=9122, help='Port number')
    args = parser.parse_args()
    
    app.run(host='0.0.0.0', port=args.port, debug=True)
