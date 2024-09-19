from flask import Flask, render_template, jsonify, Response, request, send_from_directory
import os
import argparse
import ast
import pandas as pd
import sys
import subprocess

def safe_literal_eval(val):
    try:
        return ast.literal_eval(val)
    except (ValueError, SyntaxError):
        return []

def kill_process_on_port(port):
    try:
        # Find the process running on the specified port
        result = subprocess.check_output(["lsof", "-t", "-i:{}".format(port)])
        pid = result.decode().strip()
        if pid:
            print("Process found, PID:", pid)
            # Kill the process
            subprocess.check_output(["kill", pid])
            print("Process has been killed")
        else:
            print("No process found on port {}.".format(port))
    except subprocess.CalledProcessError:
        print("No process found on port {}.".format(port))
    except Exception as e:
        print("An error occurred:", e)
        sys.exit(1)

app = Flask(__name__)

LOGS_DIR = 'logs'


@app.route('/tasks_completed')
def get_tasks():
    log_name = request.args.get('log_name')
    draw = int(request.args.get('draw', 1))
    start = int(request.args.get('start', 0))
    length = int(request.args.get('length', 50))
    search_value = request.args.get('search[value]', '')
    search_type = request.args.get('search[type]', '')
    timestamp_type = request.args.get('timestamp_type')

    manager_info_df = pd.read_csv(os.path.join(LOGS_DIR, log_name, 'vine-logs', 'manager_info.csv'))
    time_manager_start = manager_info_df['time_start'][0]

    task_done_df = pd.read_csv(os.path.join(LOGS_DIR, log_name, 'vine-logs', 'task_done.csv')).fillna('N/A')
    task_done_df['input_files'] = task_done_df['input_files'].apply(safe_literal_eval)
    task_done_df['output_files'] = task_done_df['output_files'].apply(safe_literal_eval)

    if timestamp_type == 'relative':
        time_columns = ['when_ready', 'time_commit_start', 'time_commit_end', 'when_running',
                        'time_worker_start', 'time_worker_end', 'when_waiting_retrieval',
                        'when_retrieved', 'when_done', 'when_next_ready', 'when_output_fully_lost']
        for col in time_columns:
            try:
                task_done_df[col] = round(task_done_df[col] - time_manager_start, 6)
            except:
                pass
    task_done_df['execution_time'] = round(task_done_df['execution_time'], 4)

    total_records = len(task_done_df)

    if search_value:
        if search_type == "task-id":
            task_done_df = task_done_df[task_done_df['task_id'] == int(search_value)]
        elif search_type == "category":
            task_done_df = task_done_df[task_done_df['category'].apply(lambda x: search_value in x)]
        elif search_type == "filename":
            task_done_df = task_done_df[task_done_df['input_files'].apply(lambda x: search_value in x) | task_done_df['output_files'].apply(lambda x: search_value in x)]
        elif search_type == "task-ids":
            search_tasks = [int(task_id) for task_id in search_value.split(',')]
            task_done_df = task_done_df[task_done_df['task_id'].apply(lambda x: x in search_tasks)]
    else:
        # handle sorting request
        order_column = request.args.get('order[0][column]', '0')
        order_dir = request.args.get('order[0][dir]', 'asc')
        column_name = request.args.get(f'columns[{order_column}][data]', 'task_id')
        if order_dir == 'asc':
            task_done_df = task_done_df.sort_values(by=column_name, ascending=True)
        else:
            task_done_df = task_done_df.sort_values(by=column_name, ascending=False)
        
    page_data = task_done_df.iloc[start:start + length].to_dict(orient='records')

    response = {
        "draw": draw,
        "data": page_data,
        "recordsTotal": total_records,
        "recordsFiltered": len(task_done_df)
    }

    return jsonify(response)


@app.route('/tasksFailed')
def get_tasks_failed():
    log_name = request.args.get('log_name')
    draw = int(request.args.get('draw', 1))
    start = int(request.args.get('start', 0))
    length = int(request.args.get('length', 50))
    search_value = request.args.get('search[value]', '')
    search_type = request.args.get('search[type]', '')
    timestamp_type = request.args.get('timestamp_type')

    manager_info_df = pd.read_csv(os.path.join(LOGS_DIR, log_name, 'vine-logs', 'manager_info.csv'))
    time_manager_start = manager_info_df['time_start'][0]

    tasks_failed_df = pd.read_csv(os.path.join(LOGS_DIR, log_name, 'vine-logs', 'task_failed_on_worker.csv')).fillna('N/A')

    if timestamp_type == 'relative':
        time_columns = ['when_ready', 'when_running', 'when_next_ready']
        for col in time_columns:
            tasks_failed_df[col] = round(tasks_failed_df[col] - time_manager_start, 2)

    if search_value:
        if search_type == "task-id":
            tasks_failed_df = tasks_failed_df[tasks_failed_df['task_id'] == int(search_value)]
        elif search_type == "category":
            tasks_failed_df = tasks_failed_df[tasks_failed_df['category'] == search_value]
        elif search_type == "worker-id":
            tasks_failed_df = tasks_failed_df[tasks_failed_df['worker_id'] == int(search_value)]
    else:
        # handle sorting request
        order_column = request.args.get('order[0][column]', '0')
        order_dir = request.args.get('order[0][dir]', 'asc')
        column_name = request.args.get(f'columns[{order_column}][data]', 'worker_id')
        if order_dir == 'asc':
            tasks_failed_df = tasks_failed_df.sort_values(by=column_name, ascending=True)
        else:
            tasks_failed_df = tasks_failed_df.sort_values(by=column_name, ascending=False)
    
    page_data = tasks_failed_df.iloc[start:start + length].to_dict(orient='records')

    response = {
        "draw": draw,
        "data": page_data,
        "recordsTotal": len(tasks_failed_df),
        "recordsFiltered": len(tasks_failed_df)
    }
    return response

@app.route('/worker')
def get_worker_summary():
    log_name = request.args.get('log_name')
    draw = int(request.args.get('draw', 1))
    start = int(request.args.get('start', 0))
    length = int(request.args.get('length', 50))
    search_value = request.args.get('search[value]', '')
    search_type = request.args.get('search[type]', '')
    timestamp_type = request.args.get('timestamp_type')

    worker_df = pd.read_csv(os.path.join(LOGS_DIR, log_name, 'vine-logs', 'worker_summary.csv'))

    if search_value:
        pass
    else:
        # handle sorting request
        order_column = request.args.get('order[0][column]', '0')
        order_dir = request.args.get('order[0][dir]', 'asc')
        column_name = request.args.get(f'columns[{order_column}][data]', 'worker_id')
        if order_dir == 'asc':
            worker_df = worker_df.sort_values(by=column_name, ascending=True)
        else:
            worker_df = worker_df.sort_values(by=column_name, ascending=False)
    
    page_data = worker_df.iloc[start:start + length].to_dict(orient='records')

    response = {
        "draw": draw,
        "data": page_data,
        "recordsTotal": len(worker_df),
        "recordsFiltered": len(worker_df)
    }

    return response

@app.route('/dag')
def get_dag():
    log_name = request.args.get('log_name')
    draw = int(request.args.get('draw', 1))
    start = int(request.args.get('start', 0))
    length = int(request.args.get('length', 50))
    search_value = request.args.get('search[value]', '')
    search_type = request.args.get('search[type]', '')
    timestamp_type = request.args.get('timestamp_type')

    dag_df = pd.read_csv(os.path.join(LOGS_DIR, log_name, 'vine-logs', 'graph_info.csv'))
    columns_to_return = ['graph_id', 'num_tasks', 'time_completion', 'num_critical_tasks', 'critical_tasks']
    dag_df = dag_df[columns_to_return]

    if search_value:
        pass
    else:
        # handle sorting request
        order_column = request.args.get('order[0][column]', '0')
        order_dir = request.args.get('order[0][dir]', 'asc')
        column_name = request.args.get(f'columns[{order_column}][data]', 'graph_id')
        if order_dir == 'asc':
            dag_df = dag_df.sort_values(by=column_name, ascending=True)
        else:
            dag_df = dag_df.sort_values(by=column_name, ascending=False)
    
    page_data = dag_df.iloc[start:start + length].to_dict(orient='records')

    response = {
        "draw": draw,
        "data": page_data,
        "recordsTotal": len(dag_df),
        "recordsFiltered": len(dag_df)
    }
    return response


@app.route('/file')
def get_file():
    log_name = request.args.get('log_name')
    draw = int(request.args.get('draw', 1))
    start = int(request.args.get('start', 0))
    length = int(request.args.get('length', 50))
    search_value = request.args.get('search[value]', '')
    search_type = request.args.get('search[type]', '')

    file_info_df = pd.read_csv(os.path.join(LOGS_DIR, log_name, 'vine-logs', 'file_info.csv'))
    file_info_df['producers'] = file_info_df['producers'].apply(safe_literal_eval)
    file_info_df['consumers'] = file_info_df['consumers'].apply(safe_literal_eval)
    file_info_df['worker_holding'] = file_info_df['worker_holding'].apply(safe_literal_eval)

    if search_type:
        if search_type == 'filename':
            search_value = search_value.strip()
            file_info_df = file_info_df[file_info_df['filename'].apply(lambda x: search_value in x)]
        elif search_type == 'has-producer':
            file_info_df = file_info_df[file_info_df['producers'].apply(lambda x: len(x) > 0)]
        # sort by size (temporarily)
        file_info_df = file_info_df.sort_values(by='size(MB)', ascending=True)
    else:
        # handle sorting request
        order_column = request.args.get('order[0][column]', '0')
        order_dir = request.args.get('order[0][dir]', 'asc')
        column_name = request.args.get(f'columns[{order_column}][data]', 'filename')
        if order_dir == 'asc':
            file_info_df = file_info_df.sort_values(by=column_name, ascending=True)
        else:
            file_info_df = file_info_df.sort_values(by=column_name, ascending=False)
    
    page_data = file_info_df.iloc[start:start + length].to_dict(orient='records')

    response = {
        "draw": draw,
        "data": page_data,
        "recordsTotal": len(file_info_df),
        "recordsFiltered": len(file_info_df)
    }
    return response

@app.route('/logs/<log_name>/get_csv_data', methods=['GET'])
def get_csv_data(log_name):
    csv_filename = request.args.get('csv_filename', type=str)
    file_path = os.path.join(LOGS_DIR, log_name, 'vine-logs', csv_filename)

    try:
        df = pd.read_csv(file_path).fillna('N/A')
        for col in ['input_files', 'output_files', 'producers', 'consumers', 'worker_holding', 'critical_tasks']:
            if col in df.columns:
                df[col] = df[col].apply(safe_literal_eval)
        
        return jsonify(df.to_dict(orient='records'))
    except FileNotFoundError:
        return jsonify({'error': f'File {csv_filename} not found in log {log_name}'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/logs/<log_name>')
def render_log_page(log_name):
    log_folders = [name for name in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    log_folders_sorted = sorted(log_folders)
    return render_template('index.html', log_folders=log_folders_sorted)


@app.route('/')
def index():
    log_folders = [name for name in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    log_folders_sorted = sorted(log_folders)
    return render_template('index.html', log_folders=log_folders_sorted)

@app.route('/report')
def report():
    return render_template('report.html')

@app.route('/debug')
def debug():
    return render_template('debug.html')

@app.route('/performance')
def performance():
    return render_template('performance.html')

@app.route('/transactions')
def transactions():
    return render_template('transactions.html')

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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default=9122)
    args = parser.parse_args()

    kill_process_on_port(args.port)
        
    app.run(host='0.0.0.0', port=args.port, debug=True)
