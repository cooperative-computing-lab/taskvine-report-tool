from routes.runtime_template import runtime_template_bp
from routes.subgraphs import subgraphs_bp
from routes.worker_storage_consumption import worker_storage_consumption_bp
from routes.file_sizes import file_sizes_bp
from routes.file_replicas import file_replicas_bp
from routes.task_concurrency import task_concurrency_bp
from routes.task_execution_time import task_execution_time_bp
from routes.task_execution_details import task_execution_details_bp
from routes.task_response_time import task_response_time_bp
from routes.task_retrieval_time import task_retrieval_time_bp
from routes.task_dependents import task_dependents_bp
from routes.worker_concurrency import worker_concurrency_bp
from routes.worker_executing_tasks import worker_executing_tasks_bp
from routes.worker_waiting_retrieval_tasks import worker_waiting_retrieval_tasks_bp
from routes.file_transferred_size import file_transferred_size_bp
from routes.file_created_size import file_created_size_bp
from routes.runtime_state import *
from routes.worker_transfers import worker_transfers_bp
from routes.task_completion_percentiles import task_completion_percentiles_bp
from routes.task_dependencies import task_dependencies_bp

import argparse
import os
import time
from flask import Flask, render_template, request

app = Flask(__name__)


def setup_request_logging(app, runtime_state):
    @app.before_request
    def log_request_info():
        runtime_state.log_request(request)
        request._start_time = time.time()

    @app.after_request
    def log_response_info(response):
        if hasattr(request, '_start_time'):
            duration = time.time() - request._start_time
            runtime_state.log_response(response, request, duration)
        else:
            runtime_state.log_response(response, request)

        if hasattr(request, '_start_time'):
            base_name = os.path.basename(request.path)
            if base_name in SERVICE_API_LISTS:
                runtime_state.api_responded[base_name] += 1
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

# files
app.register_blueprint(file_replicas_bp)
app.register_blueprint(file_sizes_bp)
app.register_blueprint(file_transferred_size_bp)
app.register_blueprint(file_created_size_bp)

# subgraphs
app.register_blueprint(subgraphs_bp)

# runtime template
app.register_blueprint(runtime_template_bp)


@app.route('/')
def index():
    log_folders = [name for name in os.listdir(
        LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    log_folders_sorted = sorted(log_folders)
    return render_template('index.html', log_folders=log_folders_sorted)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default=9122, help='Port number')
    args = parser.parse_args()

    runtime_state.log_info(f"Starting application on port {args.port}")
    app.run(host='0.0.0.0', port=args.port, debug=True, use_reloader=False)
