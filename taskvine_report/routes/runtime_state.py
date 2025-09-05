from taskvine_report.utils import *
import os
from pathlib import Path
from ..src.csv_generator import CSVManager
from .logger import Logger
import time
import traceback
import json
import polars as pl
import threading
from flask import current_app


class LeaseLock:
    def __init__(self, lease_duration_sec=20):
        self._lock = threading.Lock()
        self._expiry_time = 0
        self._lease_duration = lease_duration_sec

    def acquire(self):
        now = time.time()
        # release lock if it's expired
        if self._lock.locked() and now > self._expiry_time:
            try:
                self._lock.release()
            except RuntimeError:
                pass
        # release lock if there is no request being processed (the lock request itself is a request)
        if current_app.config["PROCESSING_REQUESTS_COUNT"] <= 1:
            try:
                self._lock.release()
            except RuntimeError:
                pass

        # acquire lock as non-blocking
        if not self._lock.acquire(blocking=False):
            return False

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


class RuntimeState(CSVManager):
    def __init__(self):
        # logs directory - will be set by app.py
        self.logs_dir = None
        super().__init__(None)

        self.task_stats = None

        self.tick_size = 12

        # for preventing multiple instances of the same runtime template
        self.template_lock = LeaseLock()

        # for preventing multiple reloads of the data
        self._metadata_fingerprint = None
        self.reload_lock = LeaseLock()

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
        with self.reload_lock:
            if self._metadata_fingerprint == get_files_fingerprint([self.csv_file_metadata]):
                return False

            self.reload_template(self.runtime_template)
            return True

    def ensure_runtime_template(self, runtime_template):
        if not runtime_template:
            return False

        if self.template_lock.is_locked():
            return False

        if self.runtime_template and runtime_template == os.path.basename(self.runtime_template):
            return True

        self.reload_template(runtime_template)
        return True
    
    def reload_template(self, runtime_template):
        # init template and data parser
        self.runtime_template = os.path.join(self.logs_dir, Path(runtime_template).name)
        super().__init__(self.runtime_template)

        # exclude library tasks
        # self.tasks = {tid: t for tid, t in self.tasks.items() if not t.is_library_task}

        # load metadata if available
        self.metadata = {}
        try:
            if os.path.exists(self.csv_file_metadata):
                df = pl.read_csv(self.csv_file_metadata, dtypes={"key": pl.Utf8, "value": pl.Utf8}, null_values=None)
                keys = df["key"].to_list()
                vals = df["value"].to_list()
                for k, v in zip(keys, vals):
                    self.metadata[k] = json.loads(v) if isinstance(v, str) else v
            else:
                self.log_warning("Metadata file not found, using empty metadata")
        except Exception as e:
            self.log_error(f"Failed to load metadata: {e}")

        # init task stats
        # self.get_task_stats()

        # init metadata fingerprint
        self._metadata_fingerprint = get_files_fingerprint([self.csv_file_metadata])
        
        # log info
        self.log_info(f"Runtime template changed to: {runtime_template}")

        return True
