#!/usr/bin/env python3
"""
vine_report command - Start TaskVine Report Web Server

This command starts the web-based visualization interface for TaskVine logs.
"""

import argparse
import os
import sys
import time
import warnings
import logging
import socket
from flask import Flask, render_template, request

# Suppress Flask development server warning
warnings.filterwarnings('ignore', message='This is a development server.*')
# Also suppress Werkzeug warnings
logging.getLogger('werkzeug').setLevel(logging.ERROR)

# Handle imports for both package and direct execution
try:
    from ..routes.runtime_template import runtime_template_bp
    from ..routes.worker_storage_consumption import worker_storage_consumption_bp
    from ..routes.file_sizes import file_sizes_bp
    from ..routes.file_concurrent_replicas import file_concurrent_replicas_bp
    from ..routes.task_concurrency import task_concurrency_bp
    from ..routes.task_execution_time import task_execution_time_bp
    from ..routes.task_execution_details import task_execution_details_bp
    from ..routes.task_response_time import task_response_time_bp
    from ..routes.task_retrieval_time import task_retrieval_time_bp
    from ..routes.task_dependents import task_dependents_bp
    from ..routes.worker_concurrency import worker_concurrency_bp
    from ..routes.worker_executing_tasks import worker_executing_tasks_bp
    from ..routes.worker_waiting_retrieval_tasks import worker_waiting_retrieval_tasks_bp
    from ..routes.worker_lifetime import worker_lifetime_bp
    from ..routes.file_transferred_size import file_transferred_size_bp
    from ..routes.file_created_size import file_created_size_bp
    from ..routes.file_retention_time import file_retention_time_bp
    from ..routes.runtime_state import runtime_state
    from ..routes.worker_transfers import worker_transfers_bp
    from ..routes.task_completion_percentiles import task_completion_percentiles_bp
    from ..routes.task_dependencies import task_dependencies_bp
    from ..routes.lock import lock_bp
    from ..routes.task_subgraphs import task_subgraphs_bp
except ImportError:
    # Handle direct execution
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    from taskvine_report.routes.runtime_template import runtime_template_bp
    from taskvine_report.routes.worker_storage_consumption import worker_storage_consumption_bp
    from taskvine_report.routes.file_sizes import file_sizes_bp
    from taskvine_report.routes.file_concurrent_replicas import file_concurrent_replicas_bp
    from taskvine_report.routes.task_concurrency import task_concurrency_bp
    from taskvine_report.routes.task_execution_time import task_execution_time_bp
    from taskvine_report.routes.task_execution_details import task_execution_details_bp
    from taskvine_report.routes.task_response_time import task_response_time_bp
    from taskvine_report.routes.task_retrieval_time import task_retrieval_time_bp
    from taskvine_report.routes.task_dependents import task_dependents_bp
    from taskvine_report.routes.worker_concurrency import worker_concurrency_bp
    from taskvine_report.routes.worker_executing_tasks import worker_executing_tasks_bp
    from taskvine_report.routes.worker_waiting_retrieval_tasks import worker_waiting_retrieval_tasks_bp
    from taskvine_report.routes.worker_lifetime import worker_lifetime_bp
    from taskvine_report.routes.file_transferred_size import file_transferred_size_bp
    from taskvine_report.routes.file_created_size import file_created_size_bp
    from taskvine_report.routes.file_retention_time import file_retention_time_bp
    from taskvine_report.routes.runtime_state import runtime_state
    from taskvine_report.routes.worker_transfers import worker_transfers_bp
    from taskvine_report.routes.task_completion_percentiles import task_completion_percentiles_bp
    from taskvine_report.routes.task_dependencies import task_dependencies_bp
    from taskvine_report.routes.lock import lock_bp
    from taskvine_report.routes.task_subgraphs import task_subgraphs_bp


def create_app(logs_dir):
    """Create and configure the Flask application"""
    
    # Set the package directory for templates and static files
    package_dir = os.path.dirname(os.path.dirname(__file__))
    template_dir = os.path.join(package_dir, 'templates')
    static_dir = os.path.join(package_dir, 'static')
    
    app = Flask(__name__, 
                template_folder=template_dir,
                static_folder=static_dir)

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

    # Register blueprints
    # Tasks
    app.register_blueprint(task_execution_details_bp)
    app.register_blueprint(task_execution_time_bp)
    app.register_blueprint(task_concurrency_bp)
    app.register_blueprint(task_response_time_bp)
    app.register_blueprint(task_retrieval_time_bp)
    app.register_blueprint(task_completion_percentiles_bp)
    app.register_blueprint(task_dependencies_bp)
    app.register_blueprint(task_dependents_bp)

    # Workers
    app.register_blueprint(worker_storage_consumption_bp)
    app.register_blueprint(worker_concurrency_bp)
    app.register_blueprint(worker_transfers_bp)
    app.register_blueprint(worker_executing_tasks_bp)
    app.register_blueprint(worker_waiting_retrieval_tasks_bp)
    app.register_blueprint(worker_lifetime_bp)

    # Files
    app.register_blueprint(file_concurrent_replicas_bp)
    app.register_blueprint(file_sizes_bp)
    app.register_blueprint(file_transferred_size_bp)
    app.register_blueprint(file_created_size_bp)
    app.register_blueprint(file_retention_time_bp)

    # Subgraphs
    app.register_blueprint(task_subgraphs_bp)

    # Runtime template
    app.register_blueprint(runtime_template_bp)

    # Lock
    app.register_blueprint(lock_bp)

    @app.route('/')
    def index():
        log_folders = [name for name in os.listdir(
            runtime_state.logs_dir) if os.path.isdir(os.path.join(runtime_state.logs_dir, name))]
        log_folders_sorted = sorted(log_folders)
        return render_template('index.html', log_folders=log_folders_sorted)

    return app


def get_local_ip_addresses():
    """Get all available local IP addresses"""
    ip_addresses = ['127.0.0.1']  # Always include localhost
    
    try:
        # Get hostname
        hostname = socket.gethostname()
        
        # Get all IP addresses for this host
        for addr_info in socket.getaddrinfo(hostname, None):
            ip = addr_info[4][0]
            # Filter out IPv6 and localhost duplicates
            if ':' not in ip and ip != '127.0.0.1' and ip not in ip_addresses:
                ip_addresses.append(ip)
    except Exception:
        pass
    
    try:
        # Alternative method: connect to external address to find local IP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            if local_ip not in ip_addresses:
                ip_addresses.append(local_ip)
    except Exception:
        pass
    
    return ip_addresses


def main():
    """Main entry point for vine_report command"""
    parser = argparse.ArgumentParser(
        prog='vine_report',
        description='Start TaskVine Report web server for log visualization'
    )
    
    parser.add_argument(
        '--port', 
        type=int,
        default=9122, 
        help='Port number for the web server (default: 9122)'
    )
    
    parser.add_argument(
        '--logs-dir', 
        default=os.getcwd(),
        help='Directory containing log folders (default: current directory)'
    )
    
    parser.add_argument(
        '--host',
        default='0.0.0.0',
        help='Host address to bind to (default: 0.0.0.0)'
    )
    
    args = parser.parse_args()

    # Set logs directory in runtime state
    logs_dir = os.path.abspath(args.logs_dir)
    runtime_state.set_logs_dir(logs_dir)

    # Create Flask app
    app = create_app(logs_dir)

    runtime_state.log_info(f"Starting TaskVine Report server on {args.host}:{args.port}")
    runtime_state.log_info(f"Using logs directory: {logs_dir}")
    
    print(f"🚀 Starting TaskVine Report server...")
    print(f"   📁 Logs directory: {logs_dir}")
    print(f"   🌐 Server accessible at:")
    
    # Show all available URLs
    if args.host == '0.0.0.0':
        ip_addresses = get_local_ip_addresses()
        for ip in ip_addresses:
            if ip == '127.0.0.1':
                print(f"      -> http://localhost:{args.port}")
            else:
                print(f"      -> http://{ip}:{args.port}")
    else:
        print(f"      -> http://{args.host}:{args.port}")
    
    print(f"\nPress Ctrl+C to stop the server")

    try:
        app.run(
            host=args.host, 
            port=args.port, 
            debug=False, 
            use_reloader=False
        )
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main() 