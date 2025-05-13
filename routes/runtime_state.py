import os
from pathlib import Path
from .utils import LeaseLock
from src.data_parse import DataParser
import traceback
import functools
import time
from src.logger import Logger
from src.utils import get_file_stat, build_request_info_string, build_response_info_string
import threading
import json

LOGS_DIR = 'logs'
SAMPLING_POINTS = 10000  # at lease 3: the beginning, the end, and the global peak
SAMPLING_TASK_BARS = 100000   # how many task bars to show


def check_and_reload_data():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if runtime_state.check_pkl_files_changed():
                runtime_state.reload_data()
            
            # Get the response
            response = func(*args, **kwargs)
            
            # Calculate response size if it is in json format, otherwise return 0
            response_data = response.get_json() if hasattr(response, 'get_json') else response
            response_size = len(json.dumps(response_data)) if response_data else 0

            # Log the size
            route_name = func.__name__
            runtime_state.log_info(f"Route {route_name} response size: {response_size/1024/1024:.2f} MB")
            
            return response
        return wrapper
    return decorator


class RuntimeState:
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

        self.pkl_files_info = {}

        # set logger
        self.logger = Logger()

        self.template_lock = LeaseLock(lease_duration_sec=60)
        self.reload_lock = threading.Lock()

    @property
    def log_prefix(self):
        if self.runtime_template:
            return f"[{Path(self.runtime_template).name}]"
        else:
            return ""

    def log_info(self, message):
        self.logger.info(f"{self.log_prefix} {message}")

    def log_error(self, message):
        self.logger.error(f"{self.log_prefix} {message}")

    def log_warning(self, message):
        self.logger.warning(f"{self.log_prefix} {message}")

    def log_request(self, request):
        self.logger.info(
            f"{self.log_prefix} {build_request_info_string(request)}")

    def log_response(self, response, request, duration=None):
        self.logger.info(
            f"{self.log_prefix} {build_response_info_string(response, request, duration)}")

    def check_pkl_files_changed(self):
        with self.reload_lock:
            if not self.runtime_template:
                return False

        pkl_dir = self.data_parser.pkl_files_dir
        pkl_files = ['workers.pkl', 'files.pkl',
                     'tasks.pkl', 'manager.pkl', 'subgraphs.pkl']

        for pkl_file in pkl_files:
            file_path = os.path.join(pkl_dir, pkl_file)
            current_stat = get_file_stat(file_path)

            if not current_stat:
                continue

            if (file_path not in self.pkl_files_info or
                current_stat['mtime'] != self.pkl_files_info[file_path]['mtime'] or
                    current_stat['size'] != self.pkl_files_info[file_path]['size']):
                self.log_info(f"Detected changes in {pkl_file}")
                return True

        return False

    def reload_data(self):
        with self.reload_lock:
            try:
                self.log_info("Reloading data from checkpoint...")
                self.data_parser.restore_from_checkpoint()
                self.manager = self.data_parser.manager
                self.workers = self.data_parser.workers
                self.files = self.data_parser.files
                self.tasks = self.data_parser.tasks
                self.subgraphs = self.data_parser.subgraphs

                self.MIN_TIME = self.manager.when_first_task_start_commit
                self.MAX_TIME = self.manager.time_end

                # update the pkl files info
                pkl_dir = self.data_parser.pkl_files_dir
                pkl_files = ['workers.pkl', 'files.pkl',
                             'tasks.pkl', 'manager.pkl', 'subgraphs.pkl']
                for pkl_file in pkl_files:
                    file_path = os.path.join(pkl_dir, pkl_file)
                    info = get_file_stat(file_path)
                    if info:
                        self.pkl_files_info[file_path] = info

                self.log_info("Data reload completed successfully")
            except Exception as e:
                self.log_error(f"Error reloading data: {e}")
                traceback.print_exc()

    def change_runtime_template(self, runtime_template):
        if not runtime_template:
            return False
        if self.runtime_template and Path(runtime_template).name == Path(self.runtime_template).name:
            self.log_info(f"Runtime template already set to: {runtime_template}")
            return True

        self.runtime_template = os.path.join(os.getcwd(), LOGS_DIR, Path(runtime_template).name)
        self.log_info(f"Restoring data for runtime template: {runtime_template}")

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

        self.reload_data()

        self.log_info(f"Runtime template changed to: {runtime_template}")

        return True


runtime_state = RuntimeState()
