import os
from pathlib import Path
from collections import defaultdict
from src.data_parse import DataParser
import traceback
import functools
import time
from src.logger import Logger
from src.utils import get_file_stat, build_request_info_string, build_response_info_string
import threading

LOGS_DIR = 'logs'
SAMPLING_POINTS = 10000  # at lease 3: the beginning, the end, and the global peak
SAMPLING_TASK_BARS = 100000   # how many task bars to show

SERVICE_API_LISTS = [
    'task-execution-details',
    'task-execution-time',
    'task-concurrency',
    'worker-storage-consumption',
    'file-transfers',
    'file-sizes',
    'file-concurrent-replicas',
    'subgraphs',
]


def check_and_reload_data():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if runtime_state.check_pkl_files_changed():
                runtime_state.reload_data()
            return func(*args, **kwargs)
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

        # last time process the template change
        self.last_template_change_time = 0

        self.api_responded = defaultdict(int)

        self.runtime_template_lock = threading.Lock()
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

    def has_all_service_apis_responded(self):
        self.log_info(
            f"Current processing template: {self.runtime_template} Responded APIs: {self.api_responded.keys()}")
        return all(api in self.api_responded for api in SERVICE_API_LISTS)

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
        with self.runtime_template_lock:
            if not runtime_template:
                return False
            if self.runtime_template and Path(runtime_template).name == Path(self.runtime_template).name:
                self.log_info(
                    f"Runtime template already set to: {runtime_template}")
                return True

            # first check if previous template change is still ongoing
            # if self.runtime_template and not self.has_all_service_apis_responded():
            #     self.log_warning(
            #         f"Skipping change runtime template to {runtime_template} because we are busy serving the previous template.")
            #     return False

            # clear the api_responded because we are changing to a new runtime template
            self.api_responded.clear()

            self.last_template_change_time = time.time()

            self.runtime_template = os.path.join(
                os.getcwd(), LOGS_DIR, Path(runtime_template).name)
            self.log_info(
                f"Restoring data for runtime template: {runtime_template}")

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
