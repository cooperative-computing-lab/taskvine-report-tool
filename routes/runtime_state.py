import os
from pathlib import Path
from src.data_parse import DataParser
import functools
from .logger import Logger
from .utils import (
    build_response_info_string,
    build_request_info_string,
    get_files_fingerprint
)
import json
import time
import threading


LOGS_DIR = 'logs'
SAMPLING_POINTS = 100000  # at lease 3: the beginning, the end, and the global peak
SAMPLING_TASK_BARS = 100000   # how many task bars to show


class LeaseLock:
    def __init__(self, lease_duration_sec=60):
        self._lock = threading.Lock()
        self._expiry_time = 0
        self._lease_duration = lease_duration_sec

    def acquire(self):
        now = time.time()
        if self._lock.locked() and now > self._expiry_time:
            try:
                self._lock.release()
            except RuntimeError:
                pass

        if not self._lock.acquire(blocking=False):
            return False

        self._expiry_time = time.time() + self._lease_duration
        return True

    def release(self):
        if self._lock.locked():
            try:
                self._lock.release()
                self._expiry_time = 0
                return True
            except RuntimeError:
                return False
        return False

    def renew(self):
        if self._lock.locked():
            self._expiry_time = time.time() + self._lease_duration
            return True
        return False

    def is_locked(self):
        return self._lock.locked() and time.time() <= self._expiry_time

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


def check_and_reload_data():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            runtime_state.reload_data_if_needed()

            response = func(*args, **kwargs)

            if hasattr(response, 'get_json'):
                try:
                    response_data = response.get_json()
                except Exception:
                    response_data = None
            else:
                response_data = response

            if isinstance(response_data, (dict, list)):
                response_size = len(json.dumps(response_data)) if response_data else 0
            elif hasattr(response, 'get_data'):
                response_size = len(response.get_data())
            else:
                response_size = 0

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

        # set logger
        self.logger = Logger()

        # for preventing multiple instances of the same runtime template
        self.template_lock = LeaseLock(lease_duration_sec=60)

        # for preventing multiple reloads of the data
        self._pkl_files_fingerprint = None
        self.reload_lock = LeaseLock(lease_duration_sec=180)

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
        self.logger.info(f"{self.log_prefix} {build_request_info_string(request)}")

    def log_response(self, response, request, duration=None):
        self.logger.info(f"{self.log_prefix} {build_response_info_string(response, request, duration)}")

    def reload_data_if_needed(self):
        if not self.data_parser:
            return False
        
        if not self.data_parser.pkl_files:
            return False

        with self.reload_lock:
            if self._pkl_files_fingerprint == self._get_current_pkl_files_fingerprint():
                return False

            self.reload_template(self.runtime_template)
            return True
    
    def _get_current_pkl_files_fingerprint(self):
        if not self.data_parser or not self.data_parser.pkl_files:
            return None

        return get_files_fingerprint(self.data_parser.pkl_files)
    
    def ensure_runtime_template(self, runtime_template):
        if not runtime_template:
            return

        if self.template_lock.is_locked():
            return

        if runtime_template == os.path.basename(self.runtime_template):
            return

        self.reload_template(runtime_template)

    def reload_template(self, runtime_template):
        # init template and data parser
        self.runtime_template = os.path.join(os.getcwd(), LOGS_DIR, Path(runtime_template).name)
        self.data_parser = DataParser(self.runtime_template)

        # load data
        self.data_parser.restore_from_checkpoint()
        self.manager = self.data_parser.manager
        self.workers = self.data_parser.workers
        self.files = self.data_parser.files
        self.tasks = self.data_parser.tasks
        self.subgraphs = self.data_parser.subgraphs

        # init time range
        self.MIN_TIME = self.manager.when_first_task_start_commit
        self.MAX_TIME = self.manager.time_end

        # init pkl files fingerprint
        self._pkl_files_fingerprint = self._get_current_pkl_files_fingerprint()
        
        # log info
        self.log_info(f"Runtime template changed to: {runtime_template}")

        return True


runtime_state = RuntimeState()
