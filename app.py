import argparse
import os
import time
from flask import Flask, render_template, request

# global configuration - can be overridden via command line arguments
LOGS_DIR = os.getcwd()  # Default to current directory

# Import routes after setting LOGS_DIR
from routes.runtime_template import runtime_template_bp
from routes.worker_storage_consumption import worker_storage_consumption_bp
from routes.file_sizes import file_sizes_bp
from routes.file_concurrent_replicas import file_concurrent_replicas_bp
from routes.task_concurrency import task_concurrency_bp
from routes.task_execution_time import task_execution_time_bp
from routes.task_execution_details import task_execution_details_bp
from routes.task_response_time import task_response_time_bp
from routes.task_retrieval_time import task_retrieval_time_bp
from routes.task_dependents import task_dependents_bp
from routes.worker_concurrency import worker_concurrency_bp
from routes.worker_executing_tasks import worker_executing_tasks_bp
from routes.worker_waiting_retrieval_tasks import worker_waiting_retrieval_tasks_bp
from routes.worker_lifetime import worker_lifetime_bp
from routes.file_transferred_size import file_transferred_size_bp
from routes.file_created_size import file_created_size_bp
from routes.file_retention_time import file_retention_time_bp
from routes.runtime_state import *
from routes.worker_transfers import worker_transfers_bp
from routes.task_completion_percentiles import task_completion_percentiles_bp
from routes.task_dependencies import task_dependencies_bp
from routes.lock import lock_bp
from routes.task_subgraphs import task_subgraphs_bp

app = Flask(__name__)


def setup_request_logging(app, runtime_state):
    @app.before_request
    def log_request_info():
        runtime_state.template_lock.renew()
        request_folder = request.args.get('folder')
        runtime_state.log_request(request)
        request._start_time = time.time()
        
        runtime_state.ensure_runtime_template(request_folder)

    @app.after_request
    def log_response_info(response):
        runtime_state.template_lock.renew()
        if hasattr(request, '_start_time'):
            duration = time.time() - request._start_time
            runtime_state.log_response(response, request, duration)
        else:
            runtime_state.log_response(response, request)
        return response

    return app


setup_request_logging(app, runtime_state)

# tasks
app.register_blueprint(task_execution_details_bp)
app.register_blueprint(task_execution_time_bp)
app.register_blueprint(task_concurrency_bp)
app.register_blueprint(task_response_time_bp)
app.register_blueprint(task_retrieval_time_bp)
app.register_blueprint(task_completion_percentiles_bp)
app.register_blueprint(task_dependencies_bp)
app.register_blueprint(task_dependents_bp)

# workers
app.register_blueprint(worker_storage_consumption_bp)
app.register_blueprint(worker_concurrency_bp)
app.register_blueprint(worker_transfers_bp)
app.register_blueprint(worker_executing_tasks_bp)
app.register_blueprint(worker_waiting_retrieval_tasks_bp)
app.register_blueprint(worker_lifetime_bp)

# files
app.register_blueprint(file_concurrent_replicas_bp)
app.register_blueprint(file_sizes_bp)
app.register_blueprint(file_transferred_size_bp)
app.register_blueprint(file_created_size_bp)
app.register_blueprint(file_retention_time_bp)

# subgraphs
app.register_blueprint(task_subgraphs_bp)

# runtime template
app.register_blueprint(runtime_template_bp)

# lock
app.register_blueprint(lock_bp)


@app.route('/')
def index():
    log_folders = [name for name in os.listdir(
        runtime_state.logs_dir) if os.path.isdir(os.path.join(runtime_state.logs_dir, name))]
    log_folders_sorted = sorted(log_folders)
    return render_template('index.html', log_folders=log_folders_sorted)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default=9122, help='Port number')
    parser.add_argument('--logs-dir', default=None, help='Directory containing log folders (default: current directory)')
    args = parser.parse_args()

    # Set global LOGS_DIR from command line argument or keep default
    if args.logs_dir:
        LOGS_DIR = os.path.abspath(args.logs_dir)
    
    # Initialize runtime_state with the LOGS_DIR
    runtime_state.set_logs_dir(LOGS_DIR)

    runtime_state.log_info(f"Starting application on port {args.port}")
    runtime_state.log_info(f"Using logs directory: {LOGS_DIR}")
    app.run(host='0.0.0.0', port=args.port, debug=True, use_reloader=False)
