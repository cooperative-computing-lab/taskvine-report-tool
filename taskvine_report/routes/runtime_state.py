from taskvine_report.utils import *
import os
from pathlib import Path
from ..src.data_parser import DataParser
from .logger import Logger
import time
import traceback
import threading


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


class RuntimeState(DataParser):
    def __init__(self):
        # logs directory - will be set by app.py
        self.logs_dir = None
        super().__init__(None)

        self.task_stats = None

        self.tick_size = 12

        # for preventing multiple instances of the same runtime template
        self.template_lock = LeaseLock(lease_duration_sec=60)

        # for preventing multiple reloads of the data
        self._pkl_files_fingerprint = None
        self.reload_lock = LeaseLock(lease_duration_sec=180)

    def set_logger(self):
        self.logger = Logger()

    def set_logs_dir(self, logs_dir):
        self.logs_dir = logs_dir

    @property
    def log_prefix(self):
        if self.runtime_template:
            return f"[{Path(self.runtime_template).name}]"
        else:
            return "APP"

    def log_info(self, message):
        self.logger.info(f"{self.log_prefix} {message}")

    def log_error(self, message, with_traceback=True):
        if with_traceback:
            tb = traceback.format_exc()
            message = f"{message}\n{tb}"
        self.logger.error(f"{self.log_prefix} {message}")

    def log_warning(self, message):
        self.logger.warning(f"{self.log_prefix} {message}")

    def log_request(self, request):
        self.logger.info(f"{self.log_prefix} {build_request_info_string(request)}")

    def log_response(self, response, request, duration=None):
        self.logger.info(f"{self.log_prefix} {build_response_info_string(response, request, duration)}")

    def reload_data_if_needed(self):
        if not self.pkl_files:
            return False

        with self.reload_lock:
            if self._pkl_files_fingerprint == self._get_current_pkl_files_fingerprint():
                return False

            self.reload_template(self.runtime_template)
            return True
    
    def _get_current_pkl_files_fingerprint(self):
        if not self.pkl_files:
            return None

        return get_files_fingerprint(self.pkl_files)
    
    def ensure_runtime_template(self, runtime_template):
        if not runtime_template:
            return False

        if self.template_lock.is_locked():
            return False

        if self.runtime_template and runtime_template == os.path.basename(self.runtime_template):
            return True

        self.reload_template(runtime_template)
        return True
    
    def get_task_stats(self):
        # for calculating task dependents and dependencies
        output_file_to_task = {}
        for task in self.tasks.values():
            for f in task.output_files:
                output_file_to_task[f] = task.task_id
        dependency_map = {task.task_id: set() for task in self.tasks.values()}
        dependent_map = {task.task_id: set() for task in self.tasks.values()}

        for task in self.tasks.values():
            task_id = task.task_id
            for f in task.input_files:
                parent_id = output_file_to_task.get(f)
                if parent_id and parent_id != task_id:
                    dependency_map[task_id].add(parent_id)
                    dependent_map[parent_id].add(task_id)

        # sort all tasks by when_ready time
        sorted_tasks = sorted(
            self.tasks.values(),
            key=lambda t: (t.when_ready if t.when_ready is not None else float('inf'))
        )

        # assign global_idx to each task
        task_stats = []
        for idx, task in enumerate(sorted_tasks, 1):
            task_id = task.task_id
            task_try_id = task.task_try_id

            # calculate task response time
            if task.when_running:
                task_response_time = max(floor_decimal(task.when_running - task.when_ready, 2), 0.01)
                was_dispatched = True
            elif task.when_failure_happens:
                task_response_time = max(floor_decimal(task.when_failure_happens - task.when_ready, 2), 0.01)
                was_dispatched = False
            else:
                task_response_time = None
                was_dispatched = False

            # calculate task execution time
            if task.task_status == 0:
                task_execution_time = max(floor_decimal(task.time_worker_end - task.time_worker_start, 2), 0.01)
                ran_to_completion = True
            elif task.task_status != 0 and task.when_running and task.when_failure_happens:
                task_execution_time = max(floor_decimal(task.when_failure_happens - task.when_running, 2), 0.01)
                ran_to_completion = False
            else:
                task_execution_time = None
                ran_to_completion = None

            # calculate task waiting retrieval time
            if task.when_retrieved and task.when_waiting_retrieval:
                task_waiting_retrieval_time = max(floor_decimal(task.when_retrieved - task.when_waiting_retrieval, 2), 0.01)
            else:
                task_waiting_retrieval_time = None

            row = {
                'task_id': task_id,
                'task_try_id': task_try_id,
                'global_idx': idx,
                'task_response_time': task_response_time,
                'task_execution_time': task_execution_time,
                'task_waiting_retrieval_time': task_waiting_retrieval_time,
                'dependency_count': len(dependency_map[task_id]),
                'dependent_count': len(dependent_map[task_id]),
                'was_dispatched': was_dispatched,
                'ran_to_completion': ran_to_completion
            }
            task_stats.append(row)

        self.task_stats = task_stats

    def reload_template(self, runtime_template):
        # init template and data parser
        self.runtime_template = os.path.join(self.logs_dir, Path(runtime_template).name)
        super().__init__(self.runtime_template)

        # load data
        # self.restore_from_checkpoint()
        # exclude library tasks
        self.tasks = {tid: t for tid, t in self.tasks.items() if not t.is_library_task}

        # init task stats
        self.get_task_stats()

        # init pkl files fingerprint
        self._pkl_files_fingerprint = self._get_current_pkl_files_fingerprint()
        
        # log info
        self.log_info(f"Runtime template changed to: {runtime_template}")

        return True
