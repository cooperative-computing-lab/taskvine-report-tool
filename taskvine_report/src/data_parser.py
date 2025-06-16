import platform
import subprocess
from .worker_info import WorkerInfo
from .task_info import TaskInfo
from .file_info import FileInfo
from .manager_info import ManagerInfo

import os
import math
import pandas as pd
from functools import lru_cache
from datetime import datetime
import time
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn, BarColumn
from collections import defaultdict
import cloudpickle
from datetime import timezone, timedelta
import pytz
import platform
from taskvine_report.utils import *


def count_lines(file_name):
    if platform.system() in ["Linux", "Darwin"]:  # Linux or macOS
        try:
            return int(subprocess.check_output(["wc", "-l", file_name]).split()[0])
        except subprocess.CalledProcessError:
            pass

    with open(file_name, 'r', encoding='utf-8', errors='ignore') as f:
        return sum(1 for _ in f)


class DataParser:
    def __init__(self, runtime_template, checkpoint_pkl_files=False, debug_mode=False):
        self.runtime_template = runtime_template
        self.checkpoint_pkl_files = checkpoint_pkl_files

        self.ip = None
        self.port = None
        self.transfer_port = None

        if not self.runtime_template:
            return

        # log files
        self.vine_logs_dir = os.path.join(self.runtime_template, 'vine-logs')
        self.csv_files_dir = os.path.join(self.runtime_template, 'csv-files')
        self.json_files_dir = os.path.join(self.runtime_template, 'json-files')
        self.pkl_files_dir = os.path.join(self.runtime_template, 'pkl-files')
        self.svg_files_dir = os.path.join(self.runtime_template, 'svg-files')
        ensure_dir(self.csv_files_dir, replace=False)
        ensure_dir(self.json_files_dir, replace=False)
        ensure_dir(self.pkl_files_dir, replace=False)
        ensure_dir(self.svg_files_dir, replace=False)

        # csv files
        self.csv_file_file_concurrent_replicas = os.path.join(self.csv_files_dir, 'file_concurrent_replicas.csv')
        self.csv_file_file_created_size = os.path.join(self.csv_files_dir, 'file_created_size.csv')
        self.csv_file_file_transferred_size = os.path.join(self.csv_files_dir, 'file_transferred_size.csv')
        self.csv_file_worker_concurrency = os.path.join(self.csv_files_dir, 'worker_concurrency.csv')
        self.csv_file_retention_time = os.path.join(self.csv_files_dir, 'file_retention_time.csv')
        self.csv_file_task_execution_time = os.path.join(self.csv_files_dir, 'task_execution_time.csv')
        self.csv_file_task_response_time = os.path.join(self.csv_files_dir, 'task_response_time.csv')
        self.csv_file_task_concurrency = os.path.join(self.csv_files_dir, 'task_concurrency.csv')
        self.csv_file_task_concurrency_recovery_only = os.path.join(self.csv_files_dir, 'task_concurrency_recovery_only.csv')
        self.csv_file_task_retrieval_time = os.path.join(self.csv_files_dir, 'task_retrieval_time.csv')
        self.csv_file_task_dependencies = os.path.join(self.csv_files_dir, 'task_dependencies.csv')
        self.csv_file_task_dependents = os.path.join(self.csv_files_dir, 'task_dependents.csv')
        self.csv_file_task_completion_percentiles = os.path.join(self.csv_files_dir, 'task_completion_percentiles.csv')
        self.csv_file_sizes = os.path.join(self.csv_files_dir, 'file_sizes.csv')
        self.csv_file_worker_lifetime = os.path.join(self.csv_files_dir, 'worker_lifetime.csv')
        self.csv_file_worker_executing_tasks = os.path.join(self.csv_files_dir, 'worker_executing_tasks.csv')
        self.csv_file_worker_waiting_retrieval_tasks = os.path.join(self.csv_files_dir, 'worker_waiting_retrieval_tasks.csv')
        self.csv_file_worker_incoming_transfers = os.path.join(self.csv_files_dir, 'worker_incoming_transfers.csv')
        self.csv_file_worker_outgoing_transfers = os.path.join(self.csv_files_dir, 'worker_outgoing_transfers.csv')
        self.csv_file_worker_storage_consumption = os.path.join(self.csv_files_dir, 'worker_storage_consumption.csv')
        self.csv_file_worker_storage_consumption_percentage = os.path.join(self.csv_files_dir, 'worker_storage_consumption_percentage.csv')
        self.csv_file_task_subgraphs = os.path.join(self.csv_files_dir, 'task_subgraphs.csv')
        self.csv_file_task_execution_details = os.path.join(self.csv_files_dir, 'task_execution_details.csv')

        self.debug = os.path.join(self.vine_logs_dir, 'debug')
        self.transactions = os.path.join(self.vine_logs_dir, 'transactions')
        self.taskgraph = os.path.join(self.vine_logs_dir, 'taskgraph')
        self.daskvine_log = os.path.join(self.vine_logs_dir, 'daskvine.log')

        # these are the main files for data analysis
        self.pkl_file_names = ['workers.pkl', 'files.pkl', 'tasks.pkl', 'manager.pkl', 'subgraphs.pkl']
        self.pkl_files = []
        for pkl_file_name in self.pkl_file_names:
            self.pkl_files.append(os.path.join(self.pkl_files_dir, pkl_file_name))

        # output csv files
        self.manager = ManagerInfo()

        # tasks
        self.tasks = {}        # key: (task_id, task_try_id), value: TaskInfo
        self.current_try_id = {}   # key: task_id, value: task_try_id

        # workers
        self.workers = {}      # key: (ip, port, connect_id), value: WorkerInfo
        self.current_worker_connect_id = defaultdict(int)  # key: (ip, port), value: connect_id
        self.map_ip_and_transfer_port_to_worker_port = {}  # key: (ip, transfer_port), value: WorkerInfo

        # files
        self.files = {}      # key: file_name, value: FileInfo
        # key: (source_ip, source_port), value: TransferInfo
        self.sending_back_transfers = {}  # key: (dest_ip, dest_port), value: TransferInfo
        self.putting_transfers = {}  # key: (worker_ip, worker_port), value: TransferInfo

        # subgraphs
        self.subgraphs = {}   # key: subgraph_id, value: set()

        # status
        self.debug_mode = debug_mode
        self.receiving_resources_from_worker = None
        self.sending_task = None
        self.mini_task_transferring = None
        self.sending_back = None
        self.debug_current_line = None
        self.debug_current_parts = None
        self.debug_current_timestamp = None
        self._init_debug_handlers()

        # for plotting
        self.MIN_TIME = None
        self.MAX_TIME = None
        self.time_domain_file = os.path.join(self.csv_files_dir, 'time_domain.csv')

    def _init_debug_handlers(self):
        def H(name, cond, action):
            action.__name__ = name
            return (cond, action)

        self.debug_handlers = [

            H("send_task",
            lambda l, p, ctx: "task" in p or ctx.sending_task,
            lambda l, p, ctx: ctx._handle_debug_line_send_task_to_worker()),

            H("puturl",
            lambda l, p, ctx: "puturl" in p or "puturl_now" in p,
            lambda l, p, ctx: ctx._handle_debug_line_puturl()),

            H("cache_update",
            lambda l, p, ctx: "cache-update" in p,
            lambda l, p, ctx: ctx._handle_debug_line_cache_update()),

            H("task_state_change",
            lambda l, p, ctx: "state change:" in l,
            lambda l, p, ctx: ctx._handle_debug_line_task_state_change()),

            H("unlink",
            lambda l, p, ctx: "unlink" in p,
            lambda l, p, ctx: ctx._handle_debug_line_unlink()),

            H("worker_removed",
            lambda l, p, ctx: "removed" in p and "worker" in p,
            lambda l, p, ctx: ctx._handle_debug_line_worker_removed()),

            H("complete",
            lambda l, p, ctx: "complete" in p,
            lambda l, p, ctx: ctx._handle_debug_line_complete()),

            H("worker_received",
            lambda l, p, ctx: "received" in p,
            lambda l, p, ctx: ctx._handle_debug_line_worker_received()),

            H("receive_info",
            lambda l, p, ctx: "info" in p,
            lambda l, p, ctx: ctx._handle_debug_line_receive_worker_info(ctx.debug_current_timestamp, p)),

            H("cache_invalid",
            lambda l, p, ctx: "cache-invalid" in p,
            lambda l, p, ctx: ctx._handle_debug_line_cache_invalid()),

            H("worker_resources",
            lambda l, p, ctx: "resources" in p or ctx.receiving_resources_from_worker,
            lambda l, p, ctx: ctx._handle_debug_line_worker_resources()),

            H("stdout",
            lambda l, p, ctx: "stdout" in p,
            lambda l, p, ctx: ctx._handle_debug_line_stdout()),

            H("recovery_task",
            lambda l, p, ctx: "Submitted recovery task" in l,
            lambda l, p, ctx: ctx._handle_debug_line_submitted_recovery_task()),

            H("sending_back",
            lambda l, p, ctx: "sending back" in l or ctx.sending_back,
            lambda l, p, ctx: ctx._handle_debug_line_sending_back()),

            H("put_file",
            lambda l, p, ctx: "put" in p,
            lambda l, p, ctx: ctx._handle_debug_line_put_file()),

            H("failed_to_send_task",
            lambda l, p, ctx: "Failed to send task" in l,
            lambda l, p, ctx: ctx._handle_debug_line_failed_to_send_task()),

            H("transfer_port",
            lambda l, p, ctx: "transfer-port" in p,
            lambda l, p, ctx: ctx._handle_debug_line_get_worker_transfer_port()),

            H("mini_task",
            lambda l, p, ctx: "mini_task" in p,
            lambda l, p, ctx: ctx._handle_debug_line_mini_task()),

            H("exhausted_resources",
            lambda l, p, ctx: "exhausted" in p and "resources" in p,
            lambda l, p, ctx: ctx._handle_debug_line_exhausted_resources_on_worker()),

            H("listening",
            lambda l, p, ctx: "listening on port" in l,
            lambda l, p, ctx: ctx._handle_debug_line_listening_on_port()),

            H("worker_connected",
            lambda l, p, ctx: "worker" in p and "connected" in p,
            lambda l, p, ctx: ctx._handle_debug_line_worker_connected()),

            H("manager_end",
            lambda l, p, ctx: "manager end" in l,
            lambda l, p, ctx: ctx.manager.set_time_end(ctx.debug_current_timestamp)),

            H("remove_instances",
            lambda l, p, ctx: "Removing instances of worker" in l,
            lambda l, p, ctx: None),

        ]
        self.debug_handler_profiling = defaultdict(lambda: {"hits": 0})

    def _create_progress_bar(self):
        return Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(),
            MofNCompleteColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            refresh_per_second=10,
        )

    def write_df_to_csv(self, df, csv_file_path, **kwargs):
        df.to_csv(csv_file_path, **kwargs)

    def get_current_worker_by_ip_port(self, worker_ip: str, worker_port: int):
        connect_id = self.current_worker_connect_id[(worker_ip, worker_port)]
        if connect_id == 0:
            return None
        return self.workers[(worker_ip, worker_port, connect_id)]

    def get_current_worker_entry_by_ip_port(self, worker_ip: str, worker_port: int):
        connect_id = self.current_worker_connect_id[(worker_ip, worker_port)]
        if connect_id == 0:
            return None
        return (worker_ip, worker_port, connect_id)

    def worker_ip_port_to_hash(self, worker_ip: str, worker_port: int):
        return f"{worker_ip}:{worker_port}"

    def set_time_zone(self):
        mgr_start_datestring = None
        mgr_start_timestamp = None

        # read the first line containing "listening on port" in debug file
        with open(self.debug, 'r') as file:
            for line in file:
                if "listening on port" in line:
                    parts = line.strip().split()
                    mgr_start_datestring = f"{parts[0]} {parts[1]}"
                    mgr_start_datestring = datetime.strptime(mgr_start_datestring, "%Y/%m/%d %H:%M:%S.%f").replace(microsecond=0)
                    break

        # read the first line containing "MANAGER" and "START" in transactions file
        with open(self.transactions, 'r') as file:
            for line in file:
                if line.startswith('#'):
                    continue
                if "MANAGER" in line and "START" in line:
                    parts = line.strip().split()
                    mgr_start_timestamp = int(int(parts[0]) / 1e6)
                    break

        if mgr_start_datestring is None or mgr_start_timestamp is None:
            raise ValueError("Could not find required timestamps.")

        utc_time = datetime.fromtimestamp(mgr_start_timestamp, tz=timezone.utc)

        for tzname in pytz.all_timezones:
            tz = pytz.timezone(tzname)
            try:
                local_dt = utc_time.astimezone(tz).replace(microsecond=0)
                target_local = tz.localize(mgr_start_datestring)
                delta = abs((local_dt - target_local).total_seconds())
                if delta <= 2:
                    offset = tz.utcoffset(utc_time.replace(
                        tzinfo=None)).total_seconds() / 3600
                    self.manager.time_zone_offset_hours = int(offset)
                    self.manager.equivalent_tz = timezone(
                        timedelta(hours=self.manager.time_zone_offset_hours))
                    break
            except Exception:
                continue
        else:
            raise ValueError("Could not match to a known time zone.")

        print(
            f"Set time zone to {self.manager.equivalent_tz} offset {self.manager.time_zone_offset_hours}")

    @lru_cache(maxsize=4096)
    def datestring_to_timestamp(self, datestring):
        equivalent_datestring = datetime.strptime(datestring, "%Y/%m/%d %H:%M:%S.%f").replace(tzinfo=self.manager.equivalent_tz)
        unix_timestamp = float(equivalent_datestring.timestamp())
        return unix_timestamp

    def ensure_file_info_entry(self, file_name, size_mb, timestamp):
        if file_name not in self.files:
            self.files[file_name] = FileInfo(file_name, size_mb, timestamp)
        file = self.files[file_name]
        if size_mb > 0:
            file.set_size_mb(size_mb)
        return file

    def add_task(self, task: TaskInfo):
        assert isinstance(task, TaskInfo)
        task_entry = (task.task_id, task.task_try_id)
        if task_entry in self.tasks:
            raise ValueError(f"task {task.task_id} already exists")
        self.tasks[task_entry] = task

    def add_new_worker(self, ip, port, time_connected):
        self.current_worker_connect_id[(ip, port)] += 1
        connect_id = self.current_worker_connect_id[(ip, port)]
        worker = WorkerInfo(ip, port, connect_id)
        self.workers[(ip, port, connect_id)] = worker
        worker.add_connection(time_connected)
        worker.id = len(self.workers)
        return worker
    
    def _handle_debug_line_worker_connected(self):
        worker_idx = self.debug_current_parts.index("worker")
        ip, port = WorkerInfo.extract_ip_port_from_string(self.debug_current_parts[worker_idx + 1])
        self.add_new_worker(ip, port, self.debug_current_timestamp)
        self.manager.set_when_first_worker_connect(self.debug_current_timestamp)

    def _handle_debug_line_worker_removed(self):
        release_idx = self.debug_current_parts.index("removed")
        ip, port = WorkerInfo.extract_ip_port_from_string(self.debug_current_parts[release_idx - 1])
        worker = self.get_current_worker_by_ip_port(ip, port)
        assert worker is not None

        worker.add_disconnection(self.debug_current_timestamp)
        self.manager.update_when_last_worker_disconnect(self.debug_current_timestamp)
        
        worker_entry = (worker.ip, worker.port, worker.connect_id)
        # files on the worker are removed
        for filename in worker.active_files_or_transfers:
            self.files[filename].prune_file_on_worker_entry(worker_entry, self.debug_current_timestamp)

    def _handle_debug_line_put_file(self):
        timestamp = self.debug_current_timestamp
        parts = self.debug_current_parts

        put_idx = parts.index("put")
        ip, port = WorkerInfo.extract_ip_port_from_string(parts[put_idx - 1])
        worker_entry = self.get_current_worker_entry_by_ip_port(ip, port)
        file_name = parts[put_idx + 1]

        file_cache_level = parts[put_idx + 2]
        file_size_mb = int(parts[put_idx + 3]) / 2**20

        if file_name.startswith('buffer'):
            file_type = 4
        elif file_name.startswith('file'):
            file_type = 1
        else:
            raise ValueError(f"pending file type: {file_name}")

        file = self.ensure_file_info_entry(file_name, file_size_mb, timestamp)
        transfer = file.add_transfer('manager', worker_entry, 'manager_put', file_type, file_cache_level)
        transfer.start_stage_in(timestamp, "pending")
        assert worker_entry not in self.putting_transfers
        worker = self.workers[worker_entry]
        worker.add_active_file_or_transfer(file_name)

        self.putting_transfers[worker_entry] = transfer

    def _handle_debug_line_worker_received(self):
        parts = self.debug_current_parts
        timestamp = self.debug_current_timestamp

        dest_ip, dest_port = WorkerInfo.extract_ip_port_from_string(parts[parts.index("received") - 1])
        dest_worker_entry = self.get_current_worker_entry_by_ip_port(dest_ip, dest_port)
        # the worker must be in the putting_transfers
        assert dest_worker_entry is not None and dest_worker_entry in self.putting_transfers
        
        transfer = self.putting_transfers[dest_worker_entry]
        transfer.stage_in(timestamp, "worker_received")
        del self.putting_transfers[dest_worker_entry]

    def _handle_debug_line_failed_to_send_task(self):
        task_id = int(self.debug_current_parts[self.debug_current_parts.index("task") + 1])
        task_entry = (task_id, self.current_try_id[task_id])
        task = self.tasks[task_entry]
        task.set_task_status(self.debug_current_timestamp, 43 << 3)   # failed to dispatch

    def _handle_debug_line_puturl(self):
        parts = self.debug_current_parts
        timestamp = self.debug_current_timestamp

        if "puturl" in parts:
            transfer_event = 'puturl'
        else:
            transfer_event = 'puturl_now'

        puturl_id = parts.index("puturl") if "puturl" in parts else parts.index("puturl_now")
        file_name = parts[puturl_id + 2]
        file_cache_level = int(parts[puturl_id + 3])
        size_in_mb = int(parts[puturl_id + 4]) / 2**20

        # the file name is the name on the worker's side
        file = self.ensure_file_info_entry(file_name, size_in_mb, timestamp)

        # the destination is definitely an ip:port worker
        dest_ip, dest_port = WorkerInfo.extract_ip_port_from_string(parts[puturl_id - 1])
        dest_worker = self.get_current_worker_by_ip_port(dest_ip, dest_port)
        assert dest_worker is not None

        dest_worker_entry = (dest_worker.ip, dest_worker.port, dest_worker.connect_id)
        # the source can be a url or an ip:port
        source = parts[puturl_id + 1]
        if source.startswith('https://'):
            transfer = file.add_transfer(source, dest_worker_entry, transfer_event, 2, file_cache_level)
        elif source.startswith('workerip://'):
            source_ip, source_transfer_port = WorkerInfo.extract_ip_port_from_string(source)
            source_worker_port = self.map_ip_and_transfer_port_to_worker_port[(source_ip, source_transfer_port)]
            source_worker_entry = self.get_current_worker_entry_by_ip_port(source_ip, source_worker_port)
            assert source_worker_entry is not None

            transfer = file.add_transfer(source_worker_entry, dest_worker_entry, transfer_event, 2, file_cache_level)
        elif source.startswith('file://'):
            transfer = file.add_transfer(source, dest_worker_entry, transfer_event, 2, file_cache_level)
        else:
            raise ValueError(f"unrecognized source: {source}, line: {line}")
        
        dest_worker.add_active_file_or_transfer(file_name)

        transfer.start_stage_in(timestamp, "pending")

    def _handle_debug_line_worker_resources(self):
        parts = self.debug_current_parts

        if not self.receiving_resources_from_worker:
            if parts[-1] != "resources":
                return
            resources_idx = parts.index("resources")
            ip, port = WorkerInfo.extract_ip_port_from_string(parts[resources_idx - 1])
            self.receiving_resources_from_worker = self.get_current_worker_by_ip_port(ip, port)
        else:
            if "cores" in parts:
                self.receiving_resources_from_worker.set_cores(int(float(parts[parts.index("cores") + 1])))
            elif "memory" in parts:
                self.receiving_resources_from_worker.set_memory_mb(int(float(parts[parts.index("memory") + 1])))
            elif "disk" in parts:
                self.receiving_resources_from_worker.set_disk_mb(int(float(parts[parts.index("disk") + 1])))
            elif "gpus" in parts:
                self.receiving_resources_from_worker.set_gpus(int(float(parts[parts.index("gpus") + 1])))
            elif "end" in parts:
                self.receiving_resources_from_worker = None
            else:
                pass

    def _handle_debug_line_receive_worker_info(self, timestamp, parts):
        info_idx = parts.index("info")
        ip, port = WorkerInfo.extract_ip_port_from_string(parts[info_idx - 1])
        worker = self.get_current_worker_by_ip_port(ip, port)
        if "worker-id" in parts:
            worker.set_hash(parts[info_idx + 2])
            worker.set_machine_name(parts[info_idx - 2])
        elif "worker-end-time" in parts:
            pass
        elif "from-factory" in parts:
            pass
        else:
            pass

    def _handle_debug_line_mini_task(self):
        parts = self.debug_current_parts
        timestamp = self.debug_current_timestamp

        mini_task_idx = parts.index("mini_task")
        source = parts[mini_task_idx + 1]
        file_name = parts[mini_task_idx + 2]
        cache_level = int(parts[mini_task_idx + 3])
        file_size = int(parts[mini_task_idx + 4]) / 2**20
        dest_worker_ip, dest_worker_port = WorkerInfo.extract_ip_port_from_string(parts[mini_task_idx - 1])
        
        dest_worker_entry = self.get_current_worker_entry_by_ip_port(dest_worker_ip, dest_worker_port)
        assert dest_worker_entry is not None
        
        file = self.ensure_file_info_entry(file_name, file_size, timestamp)
        transfer = file.add_transfer(source, dest_worker_entry, 'mini_task', 2, cache_level)
        transfer.start_stage_in(timestamp, "pending")
        dest_worker = self.workers[dest_worker_entry]
        dest_worker.add_active_file_or_transfer(file_name)

    def _handle_debug_line_task_state_change(self):
        parts = self.debug_current_parts
        line = self.debug_current_line
        timestamp = self.debug_current_timestamp

        task_id = int(parts[parts.index("Task") + 1])
        if "INITIAL (0) to READY (1)" in line:                  # a brand new task
            assert task_id not in self.current_try_id
            # new task entry
            self.current_try_id[task_id] += 1
            task = TaskInfo(task_id, self.current_try_id[task_id])
            task.set_when_ready(timestamp)
            self.add_task(task)
            return
        elif "INITIAL (0) to RUNNING (2)" in line:
            # this is a library task
            if task_id not in self.current_try_id:
                self.current_try_id[task_id] = 1
                task = TaskInfo(task_id, self.current_try_id[task_id])
                task.set_when_ready(timestamp)
                self.add_task(task)
            task = self.tasks[(task_id, self.current_try_id[task_id])]
            task.set_when_running(timestamp)
            task.is_library_task = True
            return

        task_entry = (task_id, self.current_try_id[task_id])
        task = self.tasks[task_entry]
        if "READY (1) to RUNNING (2)" in line:                  # as expected
            # it could be that the task related info was unable to be sent (also comes with a "failed to send" message)
            # in this case, even the state is switched to running, there is no worker info
            if self.manager.when_first_task_start_commit is None:
                self.manager.set_when_first_task_start_commit(timestamp)
            task.set_when_running(timestamp)
            if not task.worker_entry:
                return
            else:
                # update the coremap
                worker = self.workers[task.worker_entry]
                task.committed_worker_hash = worker.hash
                task.worker_id = worker.id
                worker.run_task(task)
                # check if this is the first try
                if task_id not in self.current_try_id:
                    self.current_try_id[task_id] = 1
        elif "RUNNING (2) to RUNNING (2)" in line:
            raise ValueError(
                f"task {task_id} state change: from RUNNING (2) to RUNNING (2)")
        elif "RETRIEVED (4) to RUNNING (2)" in line:
            print(
                f"Warning: task {task_id} state change: from RETRIEVED (4) to RUNNING (2)")
            pass
        elif "RUNNING (2) to DONE (5)" in line:
            print(
                f"Warning: task {task_id} state change: from RUNNING (2) to DONE (5)")
            pass
        elif "DONE (5) to WAITING_RETRIEVAL (3)" in line:
            print(
                f"Warning: task {task_id} state change: from DONE (5) to WAITING_RETRIEVAL (3)")
            pass
        elif "RUNNING (2) to WAITING_RETRIEVAL (3)" in line:    # as expected
            task.set_when_waiting_retrieval(timestamp)
            # update the coremap
            worker = self.workers[task.worker_entry]
            worker.reap_task(task)
        elif "WAITING_RETRIEVAL (3) to RETRIEVED (4)" in line:  # as expected
            task.set_when_retrieved(timestamp)
        elif "RETRIEVED (4) to DONE (5)" in line:               # as expected
            task.set_when_done(timestamp)
            if task.worker_entry:
                worker = self.workers[task.worker_entry]
                self.manager.set_when_last_task_done(timestamp)
                worker.tasks_completed.append(task)
        elif "WAITING_RETRIEVAL (3) to READY (1)" in line or \
                "RUNNING (2) to READY (1)" in line:             # task failure
            # we need to set the task status if it was not set yet
            if not task.task_status:
                # if it was committed to a worker
                if task.worker_entry:
                    # update the worker's tasks_failed, if the task was successfully committed
                    worker = self.workers[task.worker_entry]
                    worker.tasks_failed.append(task)
                    worker.reap_task(task)
                    # it could be that the worker disconnected
                    if len(worker.time_connected) == len(worker.time_disconnected):
                        task.set_task_status(worker.time_disconnected[-1], 15 << 3)
                    # it could be that its inputs are missing
                    elif task.input_files:
                        all_inputs_ready = True
                        for input_file in task.input_files:
                            this_input_ready = False
                            for transfer in self.files[input_file].transfers:
                                if transfer.destination == task.worker_entry and transfer.stage_in is not None:
                                    this_input_ready = True
                                    break
                            if not this_input_ready:
                                all_inputs_ready = False
                                break
                        if all_inputs_ready:
                            task.set_task_status(timestamp, 1)
                    # otherwise, we donot know the reason
                    else:
                        task.set_task_status(timestamp, 4 << 3)
                else:
                    # if the task was not dispatched to a worker, set to undispatched
                    task.set_task_status(timestamp, 42 << 3)

            # create a new task entry for the next try
            self.current_try_id[task_id] += 1
            new_task = TaskInfo(task_id, self.current_try_id[task_id])
            new_task.set_when_ready(timestamp)
            self.add_task(new_task)

        elif "RUNNING (2) to RETRIEVED (4)" in line:
            task.set_when_retrieved(timestamp)
            if not task.is_library_task:
                print(f"Warning: non-library task {task_id} state change: from RUNNING (2) to RETRIEVED (4)")
            else:
                task.set_task_status(timestamp, 12 << 3)
        else:
            raise ValueError(f"unrecognized state change: {line}")
        
    def _handle_debug_line_complete(self):
        parts = self.debug_current_parts
        timestamp = self.debug_current_timestamp

        complete_idx = parts.index("complete")
        task_status = int(parts[complete_idx + 1])
        exit_status = int(parts[complete_idx + 2])
        output_length = int(parts[complete_idx + 3])
        bytes_sent = int(parts[complete_idx + 4])
        time_worker_start = floor_decimal(float(parts[complete_idx + 5]) / 1e6, 2)
        time_worker_end = floor_decimal(float(parts[complete_idx + 6]) / 1e6, 2)
        sandbox_used = None
        try:
            task_id = int(parts[complete_idx + 8])
            sandbox_used = int(parts[complete_idx + 7])
        except Exception:
            task_id = int(parts[complete_idx + 7])

        task_entry = (task_id, self.current_try_id[task_id])
        task = self.tasks[task_entry]

        task.set_task_status(timestamp, task_status)

        task.set_exit_status(exit_status)
        task.set_output_length(output_length)
        task.set_bytes_sent(bytes_sent)

        task.set_time_worker_start(time_worker_start)
        task.set_time_worker_end(time_worker_end)
        task.set_sandbox_used(sandbox_used)

    def _handle_debug_line_cache_update(self):
        parts = self.debug_current_parts
        timestamp = self.debug_current_timestamp

        # cache-update cachename, &type, &cache_level, &size, &mtime, &transfer_time, &start_time, id
        cache_update_id = parts.index("cache-update")
        file_name = parts[cache_update_id + 1]
        file_type = parts[cache_update_id + 2]
        file_cache_level = parts[cache_update_id + 3]
        size_in_mb = int(parts[cache_update_id + 4]) / 2**20
        # start_sending_time = int(parts[cache_update_id + 7]) / 1e6

        # if this is a task-generated file, it is the first time the file is cached on this worker, otherwise we only update the stage in time
        file = self.ensure_file_info_entry(file_name, size_in_mb, timestamp)

        ip, port = WorkerInfo.extract_ip_port_from_string(parts[cache_update_id - 1])
        worker_entry = self.get_current_worker_entry_by_ip_port(ip, port)

        # TODO: better handle a special case where the file is created by a previous manager
        if worker_entry is None:
            del self.files[file_name]
            return
        
        worker = self.workers[worker_entry]
        worker.add_active_file_or_transfer(file_name)

        # let the file handle the cache update
        file.cache_update(worker_entry, timestamp, file_type, file_cache_level)

    def _handle_debug_line_cache_invalid(self):
        parts = self.debug_current_parts
        timestamp = self.debug_current_timestamp

        cache_invalid_id = parts.index("cache-invalid")
        file_name = parts[cache_invalid_id + 1]
        if file_name not in self.files:
            # special case: this file was created by a previous manager
            return
        ip, port = WorkerInfo.extract_ip_port_from_string(parts[cache_invalid_id - 1])
        worker_entry = (ip, port, self.current_worker_connect_id[(ip, port)])
        file = self.files[file_name]
        file.cache_invalid(worker_entry, timestamp)

    def _handle_debug_line_unlink(self):
        parts = self.debug_current_parts
        timestamp = self.debug_current_timestamp

        unlink_id = parts.index("unlink")
        file_name = parts[unlink_id + 1]
        ip, port = WorkerInfo.extract_ip_port_from_string(parts[unlink_id - 1])
        worker_entry = self.get_current_worker_entry_by_ip_port(ip, port)
        assert worker_entry is not None

        file = self.files[file_name]
        file.unlink(worker_entry, timestamp)
        worker = self.workers[worker_entry]
        worker.remove_active_file_or_transfer(file_name)

    def _handle_debug_line_exhausted_resources_on_worker(self):
        parts = self.debug_current_parts

        exhausted_idx = parts.index("exhausted resources on")
        task_id = int(parts[exhausted_idx - 1])
        task_try_id = self.current_try_id[task_id]
        task = self.tasks[(task_id, task_try_id)]
        task.exhausted_resources = True

    def _handle_debug_line_get_worker_transfer_port(self):
        parts = self.debug_current_parts

        transfer_port_idx = parts.index("transfer-port")
        transfer_port = int(parts[transfer_port_idx + 1])
        ip, port = WorkerInfo.extract_ip_port_from_string(parts[transfer_port_idx - 1])
        worker = self.get_current_worker_by_ip_port(ip, port)
        worker.set_transfer_port(transfer_port)
        self.map_ip_and_transfer_port_to_worker_port[(ip, transfer_port)] = port

    def _handle_debug_line_sending_back(self):
        # get an output file from a worker, one worker can only send one file back at a time
        parts = self.debug_current_parts
        timestamp = self.debug_current_timestamp

        if not self.sending_back:
            self.sending_back = True
            back_idx = parts.index("back")
            file_name = parts[back_idx + 1]
            source_ip, source_port = WorkerInfo.extract_ip_port_from_string(parts[back_idx - 2])
            source_worker_entry = self.get_current_worker_entry_by_ip_port(source_ip, source_port)
            assert source_worker_entry is not None
            assert source_worker_entry not in self.sending_back_transfers

            # note that the file might be a dir which has multiple files in it, we only receive a cache-update for the 
            # dir itself, but we do not know the files in the dir yet, subfiles will be received recursively
            file = self.files[file_name]
            transfer = file.add_transfer(source_worker_entry, 'manager', 'manager_get', 1, 1)
            transfer.start_stage_in(timestamp, "pending")
            self.sending_back_transfers[source_worker_entry] = transfer
            source_worker = self.workers[source_worker_entry]
            source_worker.add_active_file_or_transfer(file_name)

            """
            if self.sending_back and "rx from" in line:
                if "file" in parts:
                    file_idx = parts.index("file")
                    file_name = parts[file_idx + 1]
                    file_size = int(parts[file_idx + 2])
                    source_ip, source_port = WorkerInfo.extract_ip_port_from_string(parts[file_idx - 1])
                    assert source_ip is not None and source_port is not None
                elif "symlink" in parts:
                    # we do not support symlinks for now
                    print(f"Warning: symlinks are not supported yet, line: {line}")
                    return
                elif "dir" in parts:
                    # if this is a dir, we will call vine_manager_get_dir_contents recursively to get all files in the dir
                    # therefore, we return here and process the subsequent lines to get files
                    return 
                elif "error" in parts:
                    print(f"Warning: error in sending back, line: {line}")
                    self.sending_back = False
                    return
                # the file might be an output file of a command-line task, which does not return a cache-update message
                # thus it might not be in self.files, so we do not check it here, and do nothing at the moment
            """
        else:
            if "sent" in parts:
                send_idx = parts.index("sent")
                source_ip, source_port = WorkerInfo.extract_ip_port_from_string(parts[send_idx - 1])
                source_worker_entry = self.get_current_worker_entry_by_ip_port(source_ip, source_port)
                assert source_worker_entry is not None
                assert source_worker_entry in self.sending_back_transfers

                transfer = self.sending_back_transfers[source_worker_entry]
                transfer.stage_in(timestamp, "manager_received")
                del self.sending_back_transfers[source_worker_entry]
                self.sending_back = False
            else:
                # "get xxx" "file xxx" "Receiving xxx" may exist in between
                pass

    def _handle_debug_line_stdout(self):
        parts = self.debug_current_parts
        if parts.index("stdout") + 3 != len(parts):
            # filter out lines like "Receiving stdout of task xxx"
            return

        stdout_idx = parts.index("stdout")
        task_id = int(parts[stdout_idx + 1])
        task_entry = (task_id, self.current_try_id[task_id])
        task = self.tasks[task_entry]
        stdout_size_mb = int(parts[stdout_idx + 2]) / 2**20
        task.set_stdout_size_mb(stdout_size_mb)

    def _handle_debug_line_submitted_recovery_task(self):
        parts = self.debug_current_parts

        task_id = int(parts[parts.index("task") + 1])
        task_try_id = self.current_try_id[task_id]
        task = self.tasks[(task_id, task_try_id)]
        task.is_recovery_task = True

    def _handle_debug_line_listening_on_port(self):
        self.manager.set_time_start(self.debug_current_timestamp)

    def _handle_debug_line_send_task_to_worker(self):
        parts = self.debug_current_parts
        line = self.debug_current_line
        timestamp = self.debug_current_timestamp

        if not self.sending_task:
            task_idx = parts.index("task")
            if task_idx + 2 != len(parts) or "tx to" not in line:
                return

            task_id = int(parts[task_idx + 1])
            if (task_id, self.current_try_id[task_id]) not in self.tasks:
                # this is a library task
                self.current_try_id[task_id] += 1
                task = TaskInfo(task_id, self.current_try_id[task_id])
                task.set_when_ready(timestamp)
                task.is_library_task = True
                self.add_task(task)

            self.sending_task = self.tasks[(task_id, self.current_try_id[task_id])]

            try:
                worker_ip, worker_port = WorkerInfo.extract_ip_port_from_string(parts[task_idx - 1])
                worker_entry = (worker_ip, worker_port, self.current_worker_connect_id[(worker_ip, worker_port)])
                if not self.sending_task.worker_entry:
                    self.sending_task.set_worker_entry(worker_entry)
            except Exception:
                raise
        else:
            if "tx to" not in line:
                return
            if "end" in parts or "failed to send" in line:
                self.sending_task = None
            elif "cores" in parts:
                self.sending_task.set_cores_requested(
                    int(float(parts[parts.index("cores") + 1])))
            elif "gpus" in parts:
                self.sending_task.set_gpus_requested(
                    int(float(parts[parts.index("gpus") + 1])))
            elif "memory" in parts:
                self.sending_task.set_memory_requested_mb(
                    int(float(parts[parts.index("memory") + 1])))
            elif "disk" in parts:
                self.sending_task.set_disk_requested_mb(
                    int(float(parts[parts.index("disk") + 1])))
            elif "category" in parts:
                self.sending_task.set_category(
                    parts[parts.index("category") + 1])
            elif "needs file" in line and "as infile" in line:
                pass
            elif "infile" in parts:
                file_name = parts[parts.index("infile") + 1]
                file = self.ensure_file_info_entry(file_name, 0, timestamp)
                if not file.is_consumer(self.sending_task):
                    file.add_consumer(self.sending_task)
                    self.sending_task.add_input_file(file_name)
            elif "outfile" in parts:
                file_name = parts[parts.index("outfile") + 1]
                file = self.ensure_file_info_entry(file_name, 0, timestamp)
                if not file.is_producer(self.sending_task):
                    file.add_producer(self.sending_task)
                    self.sending_task.add_output_file(file_name)
            elif "function_slots" in parts:
                function_slots = int(parts[parts.index("function_slots") + 1])
                self.sending_task.set_function_slots(function_slots)
            elif "cmd" in parts:
                pass
            elif "python3" in parts:
                pass
            elif "env" in parts:
                pass
            else:
                pass

    def _resort_debug_handlers(self):
        # sort handlers in-place by descending hit count
        self.debug_handlers.sort(
            key=lambda pair: -self.debug_handler_profiling.get(pair[1], {}).get("hits", 0)
        )

    def parse_debug_line(self):
        line = self.debug_current_line
        parts = self.debug_current_parts
        timestamp = self.debug_current_timestamp
        self.manager.set_current_max_time(timestamp)

        if self.debug_mode:
            for cond_fn, handler_fn in self.debug_handlers:
                if cond_fn(line, parts, self):
                    handler_fn(line, parts, self)
                    self.debug_handler_profiling[handler_fn]["hits"] += 1
                    return
        else:
            for cond_fn, handler_fn in self.debug_handlers:
                if cond_fn(line, parts, self):
                    handler_fn(line, parts, self)
                    return

    def parse_debug(self):
        self.current_try_id = defaultdict(int)
        total_lines = count_lines(self.debug)
        debug_file_size_mb = floor_decimal(os.path.getsize(self.debug) / 1024 / 1024, 2)
        unit, scale = get_size_unit_and_scale(debug_file_size_mb)
        debug_file_size_str = f"{floor_decimal(debug_file_size_mb * scale, 2)} {unit}"
        
        with self._create_progress_bar() as progress:
            task_id = progress.add_task(f"[green]Parsing debug ({debug_file_size_str})", total=total_lines)
            pbar_update_interval = 100
            resort_debug_handlers_interval = 10000
            with open(self.debug, 'rb') as file:
                for i, raw_line in enumerate(file):
                    if i % pbar_update_interval == 0:   # minimize the progress bar update frequency
                        progress.update(task_id, advance=pbar_update_interval)
                    if i % resort_debug_handlers_interval == 0:
                        self._resort_debug_handlers()
                    try:
                        self.debug_current_line = raw_line.decode('utf-8').strip()
                        self.debug_current_parts = self.debug_current_line.strip().split(" ")
                        try:
                            datestring = self.debug_current_parts[0] + " " + self.debug_current_parts[1]
                            self.debug_current_timestamp = floor_decimal(self.datestring_to_timestamp(datestring), 2)
                        except Exception:
                            # this line does not start with a timestamp, which sometimes happens
                            continue
                        self.parse_debug_line()
                    except UnicodeDecodeError:
                        print(f"Error decoding line to utf-8: {raw_line}")
                    except Exception as e:
                        print(f"Error parsing line: {self.debug_current_line}")
                        raise e
            progress.update(task_id, advance=total_lines % pbar_update_interval)

        if self.debug_mode:
            print("\n=== Handler Profiling Summary ===")
            self._resort_debug_handlers()
            print(f"{'Handler':<20} {'Hits':>10}")
            print("-" * 30)
            for cond_fn, handler_fn in self.debug_handlers:
                name = getattr(handler_fn, "__name__", repr(handler_fn))
                stats = self.debug_handler_profiling.get(handler_fn, {})
                hits = stats.get("hits", 0)
                print(f"{name:<20} {hits:>10}")

    def parse_logs(self):
        self.set_time_zone()

        # parse the debug file
        self.parse_debug()
        
        # postprocess the debug
        self.postprocess_debug()

        # generate the subgraphs
        self.generate_subgraphs()

        # checkpoint pkl files so that later analysis can be done without re-parsing the debug file
        if self.checkpoint_pkl_files:
            self.checkpoint_pkl_files()

    def generate_subgraphs(self):
        # exclude library tasks from subgraph generation to match RUNTIME_STATE filtering
        tasks_keys = set(key for key, task in self.tasks.items() if not task.is_library_task)
        parent = {key: key for key in tasks_keys}
        rank = {key: 0 for key in tasks_keys}

        def find(x):
            # find the root of the tree
            root = x
            while parent[root] != root:
                root = parent[root]
            # path compression: let all nodes on the path point to the root
            while x != root:
                next_x = parent[x]
                parent[x] = root
                x = next_x
            return root

        def union(x, y):
            root_x = find(x)
            root_y = find(y)
            if root_x == root_y:
                return
            # rank those with smaller rank
            if rank[root_x] < rank[root_y]:
                parent[root_x] = root_y
            elif rank[root_x] > rank[root_y]:
                parent[root_y] = root_x
            else:
                parent[root_y] = root_x
                rank[root_x] += 1

        for file in self.files.values():
            if not file.producers:
                continue
            # use set operations to quickly get the tasks involved
            tasks_involved = (set(file.producers) | set(
                file.consumers)) & tasks_keys
            if len(tasks_involved) <= 1:
                continue
            tasks_involved = list(tasks_involved)
            first_task = tasks_involved[0]
            for other_task in tasks_involved[1:]:
                union(first_task, other_task)

        # group tasks by the root, forming subgraphs
        subgraphs = defaultdict(set)
        for task_key in tasks_keys:  # only iterate over non-library tasks
            root = find(task_key)
            subgraphs[root].add(task_key)

        sorted_subgraphs = sorted(subgraphs.values(), key=len, reverse=True)
        self.subgraphs = {i: subgraph for i,
                          subgraph in enumerate(sorted_subgraphs, 1)}

    def checkpoint_pkl_files(self):
        with self._create_progress_bar() as progress:
            pbar = progress.add_task(f"[green]Checkpointing pkl files", total=5)

            progress.update(pbar, description=f"[green]Checkpointing workers.pkl")
            with open(os.path.join(self.pkl_files_dir, 'workers.pkl'), 'wb') as f:
                cloudpickle.dump(self.workers, f)
            progress.advance(pbar)

            progress.update(pbar, description=f"[green]Checkpointing files.pkl")
            with open(os.path.join(self.pkl_files_dir, 'files.pkl'), 'wb') as f:
                cloudpickle.dump(self.files, f)
            progress.advance(pbar)

            progress.update(pbar, description=f"[green]Checkpointing tasks.pkl")
            with open(os.path.join(self.pkl_files_dir, 'tasks.pkl'), 'wb') as f:
                cloudpickle.dump(self.tasks, f)
            progress.advance(pbar)
            
            progress.update(pbar, description=f"[green]Checkpointing manager.pkl")
            with open(os.path.join(self.pkl_files_dir, 'manager.pkl'), 'wb') as f:
                cloudpickle.dump(self.manager, f)
            progress.advance(pbar)

            progress.update(pbar, description=f"[green]Checkpointing subgraphs.pkl")
            with open(os.path.join(self.pkl_files_dir, 'subgraphs.pkl'), 'wb') as f:
                cloudpickle.dump(self.subgraphs, f)
            progress.advance(pbar)

    def postprocess_debug(self):
        # some post-processing in case the manager does not exit normally or has not finished yet
        # if the manager has not finished yet, we do something to set up the None values to make the plotting tool work
        # 1. if the manager's time_end is None, we set it to the current timestamp
        if self.manager.time_end is None:
            print(f"Manager didn't exit normally, setting manager time_end to {self.manager.current_max_time}")
            self.manager.set_time_end(self.manager.current_max_time)

        # post-processing for tasks
        for task in self.tasks.values():
            # if a task's status is None, we examine if it was dispatched
            if task.task_status is None:
                if task.when_running is None:
                    task.set_task_status(self.manager.current_max_time, 42 << 3)   # undispatched
                else:
                    task.set_task_status(self.manager.current_max_time, 4 << 3)    # unknown
            # task was retrieved but not yet done
            if task.when_done is None and task.when_retrieved is not None:
                task.set_when_done(self.manager.current_max_time)
            if task.time_worker_start and task.when_running and task.time_worker_start < task.when_running:
                #! there is a big flaw in taskvine: machines may run remotely, their returned timestamps could be in a different timezone
                #! if this happens, we temporarily modify the time_worker_start to when_running, and time_worker_end to when_waiting_retrieval
                task.set_time_worker_start(task.when_running)
                task.set_time_worker_end(task.when_waiting_retrieval)
            if task.time_worker_end and task.time_worker_start and task.time_worker_end < task.time_worker_start:
                raise ValueError(f"task {task.task_id} time_worker_end is smaller than time_worker_start: {task.time_worker_start} - {task.time_worker_end}")
            # note that the task might have not been retrieved yet
            if task.when_retrieved and task.time_worker_end and task.when_retrieved < task.time_worker_end:
                if abs(task.time_worker_end - task.when_retrieved) <= 1:
                    task.set_time_worker_end(task.when_retrieved)
                else:
                    raise ValueError(f"task {task.task_id} when_retrieved is smaller than time_worker_end: {task.time_worker_end} - {task.when_retrieved}")
        # post-processing for workers
        for worker in self.workers.values():
            # if any of the workers has no time disconnected, we set it to the manager's time_end
            if not worker.time_disconnected or len(worker.time_disconnected) == 0:
                worker.time_disconnected = [self.manager.time_end]
            # check if the time_disconnected is larger than the time_connected
            for i, (time_connected, time_disconnected) in enumerate(zip(worker.time_connected, worker.time_disconnected)):
                if time_disconnected < time_connected:
                    if time_disconnected - time_connected > 1:
                        print(f"Warning: worker {worker.ip} has a disconnected time that is smaller than the connected time")
                    else:
                        worker.time_disconnected[i] = time_connected
        # post-processing for files
        for file in self.files.values():
            for transfer in file.transfers:
                if transfer.time_stage_in is None:
                    pass
                if transfer.time_stage_out is None:
                    # set the time_stage_out as the manager's time_end
                    transfer.time_stage_out = self.manager.time_end

        # set the min and max time
        self.MIN_TIME = self.manager.when_first_task_start_commit
        self.MAX_TIME = self.manager.time_end if self.manager.time_end else self.manager.current_max_time

        if self.MAX_TIME and self.MIN_TIME:
            self.time_domain = [0, self.MAX_TIME - self.MIN_TIME]
            df = pd.DataFrame({
                'MIN_TIME': [self.MIN_TIME],
                'MAX_TIME': [self.MAX_TIME]
            })
            self.write_df_to_csv(df, self.time_domain_file, index=False)

    def restore_pkl_files(self):
        time_start = time.time()
        try:
            time_start = time.time()
            with open(os.path.join(self.pkl_files_dir, 'workers.pkl'), 'rb') as f:
                self.workers = cloudpickle.load(f)
            time_end = time.time()
            print(f"Restoring workers.pkl took {round(time_end - time_start, 4)} seconds")
            time_start = time.time()
            with open(os.path.join(self.pkl_files_dir, 'files.pkl'), 'rb') as f:
                self.files = cloudpickle.load(f)
            time_end = time.time()
            print(f"Restoring files.pkl took {round(time_end - time_start, 4)} seconds")
            time_start = time.time()
            with open(os.path.join(self.pkl_files_dir, 'tasks.pkl'), 'rb') as f:
                self.tasks = cloudpickle.load(f)
            time_end = time.time()
            print(f"Restoring tasks.pkl took {round(time_end - time_start, 4)} seconds")
            time_start = time.time()
            with open(os.path.join(self.pkl_files_dir, 'manager.pkl'), 'rb') as f:
                self.manager = cloudpickle.load(f)
            time_end = time.time()
            print(f"Restoring manager.pkl took {round(time_end - time_start, 4)} seconds")
        except Exception:
            raise ValueError("The debug file has not been successfully parsed yet")
        time_end = time.time()
        print(f"Restored workers, files, tasks, manager from checkpoint in {round(time_end - time_start, 4)} seconds")

    def restore_from_checkpoint(self):
        try:
            time_start = time.time()
            with open(os.path.join(self.pkl_files_dir, 'workers.pkl'), 'rb') as f:
                self.workers = cloudpickle.load(f)
            time_end = time.time()
            print(f"Restoring workers.pkl took {round(time_end - time_start, 4)} seconds")
            time_start = time.time()
            with open(os.path.join(self.pkl_files_dir, 'files.pkl'), 'rb') as f:
                self.files = cloudpickle.load(f)
            time_end = time.time()
            print(f"Restoring files.pkl took {round(time_end - time_start, 4)} seconds")
            time_start = time.time()
            with open(os.path.join(self.pkl_files_dir, 'tasks.pkl'), 'rb') as f:
                self.tasks = cloudpickle.load(f)
            time_end = time.time()
            print(f"Restoring tasks.pkl took {round(time_end - time_start, 4)} seconds")
            time_start = time.time()
            with open(os.path.join(self.pkl_files_dir, 'manager.pkl'), 'rb') as f:
                self.manager = cloudpickle.load(f)
            time_end = time.time()
            self.MIN_TIME = self.manager.when_first_task_start_commit
            self.MAX_TIME = self.manager.time_end
            print(f"Restoring manager.pkl took {round(time_end - time_start, 4)} seconds")
        except Exception:
            raise ValueError("The debug file has not been successfully parsed yet")
        try:
            time_start = time.time()
            with open(os.path.join(self.pkl_files_dir, 'subgraphs.pkl'), 'rb') as f:
                self.subgraphs = cloudpickle.load(f)
            time_end = time.time()
            print(f"Restoring subgraphs.pkl took {round(time_end - time_start, 4)} seconds")
        except Exception:
            raise ValueError("The subgraphs have not been generated yet")

    def generate_csv_files(self):
        # return if no tasks were dispatched
        if not self.MIN_TIME:
            return

        with self._create_progress_bar() as progress:
            task_id = progress.add_task("[green]Generating plotting data", total=6)

            self.generate_file_metrics()
            progress.advance(task_id)

            self.generate_task_metrics()
            progress.advance(task_id)

            self.generate_task_concurrency_data()
            progress.advance(task_id)

            self.generate_task_execution_details_metrics()
            progress.advance(task_id)

            self.generate_worker_metrics()
            progress.advance(task_id)

            self.generate_graph_metrics()
            progress.advance(task_id)

    def generate_worker_metrics(self):
        base_time = self.MIN_TIME
        
        worker_lifetime_entries = []
        connect_events = []
        disconnect_events = []
        executing_task_events = defaultdict(list)
        waiting_retrieval_events = defaultdict(list)
        
        for worker in self.workers.values():
            worker_key = worker.get_worker_key()
            worker_ip_port = ':'.join(worker_key.split(':')[:-1])  # Remove connect_id
            worker_id = worker.id
            worker_entry = (worker.ip, worker.port, worker.connect_id)
            
            for i, t_start in enumerate(worker.time_connected):
                t_end = (
                    worker.time_disconnected[i]
                    if i < len(worker.time_disconnected)
                    else self.MAX_TIME
                )
                t0 = floor_decimal(max(0, t_start - base_time), 2)
                t1 = floor_decimal(max(0, t_end - base_time), 2)
                duration = floor_decimal(max(0, t1 - t0), 2)
                worker_lifetime_entries.append((t0, duration, worker_id, worker_ip_port))
            
            for t in worker.time_connected:
                connect_events.append(floor_decimal(t - base_time, 2))
            for t in worker.time_disconnected:
                disconnect_events.append(floor_decimal(t - base_time, 2))
        
        for task in self.tasks.values():
            if not task.worker_entry:
                continue
            
            worker_entry = task.worker_entry
            
            if task.time_worker_start and task.time_worker_end:
                start = floor_decimal(task.time_worker_start - base_time, 2)
                end = floor_decimal(task.time_worker_end - base_time, 2)
                if start < end:
                    executing_task_events[worker_entry].extend([(start, 1), (end, -1)])
            
            if task.when_waiting_retrieval and task.when_retrieved:
                start = floor_decimal(task.when_waiting_retrieval - base_time, 2)
                end = floor_decimal(task.when_retrieved - base_time, 2)
                if start < end:
                    waiting_retrieval_events[worker_entry].extend([(start, 1), (end, -1)])

        # Helper function for worker time series data
        def generate_worker_time_series_csv(events_dict, csv_file):
            if not events_dict:
                return

            column_data = {}
            time_set = set()

            for worker_entry, events in events_dict.items():
                w = self.workers.get(worker_entry)
                if w:
                    t_connected = floor_decimal(w.time_connected[0] - base_time, 2)
                    t_disconnected = floor_decimal(w.time_disconnected[0] - base_time, 2)
                    boundary = []
                    if t_connected > 0:
                        boundary.append((t_connected, 0))
                    if t_disconnected > 0:
                        boundary.append((t_disconnected, 0))
                    events += boundary

                if not events:
                    continue

                df = pd.DataFrame(events, columns=['time', 'delta'])
                df = df.groupby('time', as_index=False)['delta'].sum()
                df['cumulative'] = df['delta'].cumsum().clip(lower=0)
                
                if df['cumulative'].isna().all():
                    continue

                raw_points = df[['time', 'cumulative']].values.tolist()
                compressed_points = compress_time_based_critical_points(raw_points)

                wid = f"{worker_entry[0]}:{worker_entry[1]}:{worker_entry[2]}"
                col_map = {t: v for t, v in compressed_points}
                column_data[wid] = col_map
                time_set.update(col_map.keys())

            if not column_data:
                return

            sorted_times = sorted(time_set)
            rows = []
            for t in sorted_times:
                row = {'Time (s)': floor_decimal(t, 2)}
                for c in sorted(column_data.keys()):
                    row[c] = column_data[c].get(t, float('nan'))
                rows.append(row)

            self.write_df_to_csv(pd.DataFrame(rows), csv_file, index=False)

        # Write CSV files
        
        # 1. Worker Lifetime
        if worker_lifetime_entries:
            worker_lifetime_entries.sort(key=lambda x: x[0])
            rows = [(worker_id, worker_ip_port, duration) for _, duration, worker_id, worker_ip_port in worker_lifetime_entries]
            self.write_df_to_csv(pd.DataFrame(rows, columns=['ID', 'Worker IP Port', 'LifeTime (s)']), self.csv_file_worker_lifetime, index=False)
        
        # 2. Worker Concurrency
        initial_active = sum(1 for t in connect_events if t <= 0)
        events = (
            [(t, 1) for t in connect_events if t > 0] +
            [(t, -1) for t in disconnect_events if t > 0]
        )
        
        if events or initial_active > 0:
            df = pd.DataFrame(events, columns=["time", "delta"])
            df = df.groupby("time", as_index=False)["delta"].sum().sort_values("time")
            
            df.loc[-1] = [0.0, 0]
            df = df.sort_index().reset_index(drop=True)
            
            df["active"] = df["delta"].cumsum() + initial_active
            
            max_time = floor_decimal(self.MAX_TIME - base_time, 2)
            if df.iloc[-1]["time"] < max_time:
                last_active = df.iloc[-1]["active"]
                new_row = pd.DataFrame({"time": [max_time], "delta": [0], "active": [last_active]})
                df = pd.concat([df, new_row], ignore_index=True)
            
            export_df = df[['time', 'active']].copy()
            export_df.columns = ['Time (s)', 'Active Workers (count)']
            self.write_df_to_csv(export_df, self.csv_file_worker_concurrency, index=False)
        
        # 3. Worker Executing Tasks
        generate_worker_time_series_csv(executing_task_events, self.csv_file_worker_executing_tasks)
        
        # 4. Worker Waiting Retrieval Tasks
        generate_worker_time_series_csv(waiting_retrieval_events, self.csv_file_worker_waiting_retrieval_tasks)

    def generate_task_concurrency_data(self):
        filtered_tasks = [task for task in self.tasks.values() if not task.is_library_task]
        if not filtered_tasks:
            return

        sorted_tasks = sorted(filtered_tasks, key=lambda t: (t.when_ready or float('inf')))
        base_time = self.MIN_TIME

        task_phases = defaultdict(list)
        phase_titles = {
            'tasks_waiting': 'Waiting',
            'tasks_committing': 'Committing',
            'tasks_executing': 'Executing',
            'tasks_retrieving': 'Retrieving',
            'tasks_done': 'Done'
        }

        for task in sorted_tasks:
            ready = task.when_ready
            running = task.when_running
            start = task.time_worker_start
            end = task.time_worker_end
            fail = task.when_failure_happens
            wait_retrieval = task.when_waiting_retrieval
            done = task.when_done

            def fd(t): return floor_decimal(t - base_time, 2) if t else None

            def add_phase(name, t0, t1=None):
                if t0:
                    task_phases[name].append((fd(t0), 1))
                if t0 and t1:
                    task_phases[name].append((fd(t1), -1))

            add_phase('tasks_waiting', ready, running or fail)
            add_phase('tasks_committing', running, start or fail or wait_retrieval)
            add_phase('tasks_executing', start, end or fail or wait_retrieval)
            add_phase('tasks_retrieving', end, wait_retrieval or fail)
            if done:
                task_phases['tasks_done'].append((fd(done), 1))

        all_times = sorted({t for df in [pd.DataFrame(events, columns=['time', 'event']) for events in task_phases.values() if events] for t in df['time']})
        if not all_times:
            return
            
        time_df = pd.DataFrame({'Time (s)': all_times})
        first_time = all_times[0]

        for name, events in task_phases.items():
            if not events:
                time_df[phase_titles[name]] = 0
                continue
            df_phase = pd.DataFrame(events, columns=['time', 'event']).sort_values('time')
            df_phase = df_phase.groupby('time', as_index=False)['event'].sum()
            df_phase['cumulative'] = df_phase['event'].cumsum().clip(lower=0)
            df_phase = df_phase[['time', 'cumulative']].rename(columns={'cumulative': phase_titles[name]})
            time_df = pd.merge_asof(time_df, df_phase, left_on='Time (s)', right_on='time', direction='backward')
            time_df.drop(columns=['time'], inplace=True)

        # Ensure all phase columns exist and set first time point to 0
        for title in phase_titles.values():
            if title not in time_df.columns:
                time_df[title] = 0
            time_df.loc[time_df['Time (s)'] == first_time, title] = 0

        time_df.fillna(0, inplace=True)
        self.write_df_to_csv(time_df, self.csv_file_task_concurrency, index=False)

        recovery_phases = defaultdict(list)
        for task in sorted_tasks:
            if not task.is_recovery_task:
                continue

            ready = task.when_ready
            running = task.when_running
            start = task.time_worker_start
            end = task.time_worker_end
            fail = task.when_failure_happens
            wait_retrieval = task.when_waiting_retrieval
            done = task.when_done

            def fd(t): return floor_decimal(t - base_time, 2) if t else None

            def add_phase(name, t0, t1=None):
                if t0:
                    recovery_phases[name].append((fd(t0), 1))
                if t0 and t1:
                    recovery_phases[name].append((fd(t1), -1))

            add_phase('tasks_waiting', ready, running or fail)
            add_phase('tasks_committing', running, start or fail or wait_retrieval)
            add_phase('tasks_executing', start, end or fail or wait_retrieval)
            add_phase('tasks_retrieving', end, wait_retrieval or fail)
            if done:
                recovery_phases['tasks_done'].append((fd(done), 1))

        all_times = sorted({t for df in [pd.DataFrame(events, columns=['time', 'event']) for events in recovery_phases.values() if events] for t in df['time']})
        if not all_times:
            return
            
        time_df = pd.DataFrame({'Time (s)': all_times})
        first_time = all_times[0]

        for name, events in recovery_phases.items():
            if not events:
                time_df[phase_titles[name]] = 0
                continue
            df_phase = pd.DataFrame(events, columns=['time', 'event']).sort_values('time')
            df_phase = df_phase.groupby('time', as_index=False)['event'].sum()
            df_phase['cumulative'] = df_phase['event'].cumsum().clip(lower=0)
            df_phase = df_phase[['time', 'cumulative']].rename(columns={'cumulative': phase_titles[name]})
            time_df = pd.merge_asof(time_df, df_phase, left_on='Time (s)', right_on='time', direction='backward')
            time_df.drop(columns=['time'], inplace=True)

        # Ensure all phase columns exist and set first time point to 0
        for title in phase_titles.values():
            if title not in time_df.columns:
                time_df[title] = 0
            time_df.loc[time_df['Time (s)'] == first_time, title] = 0

        time_df.fillna(0, inplace=True)
        self.write_df_to_csv(time_df, self.csv_file_task_concurrency_recovery_only, index=False)

    def generate_task_metrics(self):
        filtered_tasks = [task for task in self.tasks.values() if not task.is_library_task]
        if not filtered_tasks:
            return

        sorted_tasks = sorted(filtered_tasks, key=lambda t: (t.when_ready or float('inf')))
        base_time = self.MIN_TIME

        execution_time_rows = []
        response_time_rows = []
        retrieval_time_rows = []
        dependencies_rows = []
        dependents_rows = []
        finish_times = []

        output_to_task = {f: t.task_id for t in filtered_tasks for f in t.output_files}
        dependency_map = defaultdict(set)
        dependent_map = defaultdict(set)

        for task in filtered_tasks:
            task_id = task.task_id
            for f in task.input_files:
                parent_id = output_to_task.get(f)
                if parent_id and parent_id != task_id:
                    dependency_map[task_id].add(parent_id)
                    dependent_map[parent_id].add(task_id)

        for idx, task in enumerate(sorted_tasks, 1):
            tid = task.task_id
            try_id = task.task_try_id
            status = task.task_status

            ready = task.when_ready
            running = task.when_running
            start = task.time_worker_start
            end = task.time_worker_end
            fail = task.when_failure_happens
            retrieved = task.when_retrieved
            wait_retrieval = task.when_waiting_retrieval
            done = task.when_done

            def fd(t): return floor_decimal(t - base_time, 2) if t else None

            et = None
            ran = 0
            if status == 0 and end and start:
                et = max(fd(end) - fd(start), 0.01)
                ran = 1
            elif running and fail:
                et = max(fd(fail) - fd(running), 0.01)
            if et:
                execution_time_rows.append((idx, et, tid, try_id, ran))

            rt = None
            dispatched = 0
            if running and ready:
                rt = max(fd(running) - fd(ready), 0.01)
                dispatched = 1
            elif fail and ready:
                rt = max(fd(fail) - fd(ready), 0.01)
            if rt is not None:
                response_time_rows.append((idx, rt, tid, try_id, dispatched))

            if retrieved and wait_retrieval:
                rtt = max(fd(retrieved) - fd(wait_retrieval), 0.01)
                retrieval_time_rows.append((idx, rtt, tid, try_id))

            dependencies_rows.append((idx, len(dependency_map[tid])))
            dependents_rows.append((idx, len(dependent_map[tid])))

            finish = done or retrieved
            if finish:
                finish_times.append(fd(finish))

        def write_csv(data, cols, path):
            if data:
                self.write_df_to_csv(pd.DataFrame(data, columns=cols), path, index=False)

        write_csv(execution_time_rows, ['Global Index', 'Execution Time', 'Task ID', 'Task Try ID', 'Ran to Completion'], self.csv_file_task_execution_time)
        write_csv(response_time_rows, ['Global Index', 'Response Time', 'Task ID', 'Task Try ID', 'Was Dispatched'], self.csv_file_task_response_time)
        write_csv(retrieval_time_rows, ['Global Index', 'Retrieval Time', 'Task ID', 'Task Try ID'], self.csv_file_task_retrieval_time)
        write_csv(dependencies_rows, ['Global Index', 'Dependency Count'], self.csv_file_task_dependencies)
        write_csv(dependents_rows, ['Global Index', 'Dependent Count'], self.csv_file_task_dependents)

        if finish_times:
            finish_times.sort()
            n = len(finish_times)
            percentiles = [(p, floor_decimal(finish_times[min(n - 1, max(0, math.ceil(p / 100 * n) - 1))], 2)) for p in range(1, 101)]
            write_csv(percentiles, ['Percentile', 'Completion Time'], self.csv_file_task_completion_percentiles)

    def generate_graph_metrics(self):
        unique_tasks = {}
        task_failure_counts = {}
        
        for (tid, try_id), task in self.tasks.items():
            if getattr(task, 'is_library_task', False):
                continue
            
            if tid not in task_failure_counts:
                task_failure_counts[tid] = 0
            
            task_status = getattr(task, 'task_status', None)
            if task_status is not None and task_status != 0:
                task_failure_counts[tid] += 1

            if tid not in unique_tasks:
                unique_tasks[tid] = task
            else:
                current_task = unique_tasks[tid]
                
                current_output_count = len(getattr(current_task, 'output_files', []))
                new_output_count = len(getattr(task, 'output_files', []))
                
                if new_output_count > 0 and current_output_count == 0:
                    unique_tasks[tid] = task
                elif new_output_count > current_output_count:
                    unique_tasks[tid] = task
                elif new_output_count == current_output_count:
                    current_status = getattr(current_task, 'task_status', None)
                    new_status = getattr(task, 'task_status', None)
                    
                    if (current_status != 0 and new_status == 0):
                        unique_tasks[tid] = task
                    elif (current_status == new_status and task.task_try_id > current_task.task_try_id):
                        unique_tasks[tid] = task

        if not unique_tasks:
            return

        # Step 2: Build set of files that have producer tasks
        files_with_producers = set()
        for task in unique_tasks.values():
            for file_name in getattr(task, 'output_files', []):
                files_with_producers.add(file_name)

        # Step 3: Map tasks to subgraphs
        task_to_subgraph = {}
        if hasattr(self, 'subgraphs') and self.subgraphs:
            for subgraph_id, task_entries in self.subgraphs.items():
                for (tid, try_id) in task_entries:
                    if tid in unique_tasks:  # Only include if we have this task in unique_tasks
                        task_to_subgraph[tid] = subgraph_id

        # Step 4: Generate the single CSV with filtered files and timing info
        rows = []
        for task in unique_tasks.values():
            task_id = task.task_id
            subgraph_id = task_to_subgraph.get(task_id, 0)  # Default to 0 if not in any subgraph
            is_recovery_task = getattr(task, 'is_recovery_task', False)
            
            # Get failure count for this task_id
            failure_count = task_failure_counts.get(task_id, 0)
            
            # Calculate input files with waiting times
            input_files_with_timing = []
            for file_name in getattr(task, 'input_files', []):
                if file_name in files_with_producers and file_name in self.files:
                    file_obj = self.files[file_name]
                    # File waiting time: from file created to task when_running
                    if task.when_running and file_obj.created_time:
                        waiting_time = max(0, task.when_running - file_obj.created_time)
                        input_files_with_timing.append(f"{file_name}:{waiting_time:.2f}")
                    else:
                        input_files_with_timing.append(f"{file_name}:0.00")
            
            # Calculate output files with creation times
            output_files_with_timing = []
            for file_name in getattr(task, 'output_files', []):
                if file_name in files_with_producers and file_name in self.files:
                    file_obj = self.files[file_name]
                    creation_time = 0.0
                    
                    # Find the cache-update time for this specific task
                    if task.worker_entry and task.time_worker_start:
                        # Look for cache-update transfers from this specific worker at the right time
                        for transfer in file_obj.transfers:
                            if (transfer.destination == task.worker_entry and 
                                transfer.time_stage_in and 
                                transfer.time_stage_in >= task.time_worker_start):
                                creation_time = max(0, transfer.time_stage_in - task.time_worker_start)
                                break
                        
                        # If no specific transfer found, use general file created time
                        if creation_time == 0.0 and file_obj.created_time and task.time_worker_start:
                            creation_time = max(0, file_obj.created_time - task.time_worker_start)
                    
                    output_files_with_timing.append(f"{file_name}:{creation_time:.2f}")
            
            # Join files with | separator
            input_files_str = '|'.join(input_files_with_timing) if input_files_with_timing else ''
            output_files_str = '|'.join(output_files_with_timing) if output_files_with_timing else ''
            
            rows.append([
                subgraph_id,
                task_id,
                is_recovery_task,
                failure_count,
                input_files_str,
                output_files_str
            ])

        if rows:
            import pandas as pd
            df = pd.DataFrame(rows, columns=[
                'subgraph_id', 'task_id', 'is_recovery_task', 'failure_count', 'input_files', 'output_files'
            ])
            self.write_df_to_csv(df, self.csv_file_task_subgraphs, index=False)

    def generate_file_metrics(self):
        base_time = self.MIN_TIME

        rows_concurrent = []
        events_created = []
        events_transferred = []
        rows_retention = []
        rows_sizes = []
        max_size = 0

        all_worker_storage = defaultdict(list)
        worker_transfer_events = {
            'incoming': defaultdict(list),
            'outgoing': defaultdict(list)
        }

        for file in self.files.values():
            if not file.transfers or not file.producers:
                continue

            intervals = [
                (t.time_stage_in, t.time_stage_out)
                for t in file.transfers
                if t.time_stage_in and t.time_stage_out
            ]
            if intervals:
                events = [(start, 1) for start, _ in intervals] + [(end, -1) for _, end in intervals]
                events.sort()
                count = max_simul = 0
                for _, delta in events:
                    count += delta
                    max_simul = max(max_simul, count)
            else:
                max_simul = 0
            rows_concurrent.append((file.filename, max_simul, file.created_time))

            stage_times = [t.time_stage_in for t in file.transfers if t.time_stage_in is not None]
            if stage_times:
                first_time = min(stage_times)
                t = floor_decimal(float(first_time - base_time), 2)
                events_created.append((t, file.size_mb))

            for transfer in file.transfers:
                if transfer.time_stage_in:
                    t = floor_decimal(float(transfer.time_stage_in - base_time), 2)
                    events_transferred.append((t, file.size_mb))
                elif transfer.time_stage_out:
                    t = floor_decimal(float(transfer.time_stage_out - base_time), 2)
                    events_transferred.append((t, file.size_mb))

                dest = transfer.destination
                if isinstance(dest, tuple) and transfer.time_stage_in:
                    time_in = floor_decimal(transfer.time_start_stage_in - base_time, 2)
                    time_out = floor_decimal(transfer.time_stage_out - base_time, 2)
                    size = max(0, file.size_mb)
                    all_worker_storage[dest].extend([(time_in, size), (time_out, -size)])

                for role in ['incoming', 'outgoing']:
                    wid = getattr(transfer, 'destination' if role == 'incoming' else 'source', None)
                    if not isinstance(wid, tuple):
                        continue
                    t0 = floor_decimal(transfer.time_start_stage_in - base_time, 2)
                    t1 = None
                    if transfer.time_stage_in:
                        t1 = floor_decimal(transfer.time_stage_in - base_time, 2)
                    elif transfer.time_stage_out:
                        t1 = floor_decimal(transfer.time_stage_out - base_time, 2)
                    if t1 is not None:
                        worker_transfer_events[role][wid].extend([(t0, 1), (t1, -1)])

            first_stage_in = min((t.time_start_stage_in for t in file.transfers if t.time_start_stage_in), default=float('inf'))
            last_stage_out = max((t.time_stage_out for t in file.transfers if t.time_stage_out), default=float('-inf'))
            if first_stage_in != float('inf') and last_stage_out != float('-inf'):
                retention_time = floor_decimal(last_stage_out - first_stage_in, 2)
                rows_retention.append((file.filename, retention_time, file.created_time))

            fname = file.filename
            size = file.size_mb
            if size is not None:
                created_time = min((t.time_start_stage_in for t in file.transfers if t.time_start_stage_in), default=float('inf'))
                if created_time != float('inf'):
                    rows_sizes.append((fname, size, created_time))
                    max_size = max(max_size, size)

        # --- output ---

        def write_df(df, columns, file):
            df.columns = columns
            self.write_df_to_csv(df, file, index=False)

        if rows_concurrent:
            df = pd.DataFrame(rows_concurrent, columns=['file_name', 'max_simul_replicas', 'created_time'])
            df = df.sort_values(by='created_time')
            df.insert(0, 'file_idx', range(1, len(df) + 1))
            df = df[['file_idx', 'file_name', 'max_simul_replicas']]
            write_df(df, ['File Index', 'File Name', 'Max Concurrent Replicas (count)'], self.csv_file_file_concurrent_replicas)

        if events_created:
            df = pd.DataFrame(events_created, columns=['time', 'delta'])
            df['time'] = df['time'].apply(lambda x: floor_decimal(x, 2))
            df = df.groupby('time', as_index=False)['delta'].sum()
            df['cumulative'] = df['delta'].cumsum().clip(lower=0)
            df.insert(0, 'file_idx', range(1, len(df) + 1))
            df = df[['file_idx', 'time', 'cumulative']]
            write_df(df, ['File Index', 'Time (s)', 'Cumulative Size (MB)'], self.csv_file_file_created_size)

        if events_transferred:
            df = pd.DataFrame(events_transferred, columns=["time", "delta"])
            df["time"] = df["time"].apply(lambda x: floor_decimal(x, 2))
            df = df.groupby("time", as_index=False)["delta"].sum()
            df["cumulative"] = df["delta"].cumsum().clip(lower=0)
            write_df(df[["time", "cumulative"]], ['Time (s)', 'Cumulative Size (MB)'], self.csv_file_file_transferred_size)

        if rows_retention:
            df = pd.DataFrame(rows_retention, columns=['file_name', 'retention_time', 'created_time'])
            df = df.sort_values(by='created_time')
            df.insert(0, 'file_idx', range(1, len(df) + 1))
            df = df[['file_idx', 'file_name', 'retention_time']]
            write_df(df, ['File Index', 'File Name', 'Retention Time (s)'], self.csv_file_retention_time)

        if rows_sizes:
            df = pd.DataFrame(rows_sizes, columns=['file_name', 'file_size', 'created_time'])
            df = df.sort_values(by='created_time')
            df.insert(0, 'file_idx', range(1, len(df) + 1))
            unit, scale = get_size_unit_and_scale(max_size)
            df['file_size_scaled'] = df['file_size'] * scale
            df = df[['file_idx', 'file_name', 'file_size_scaled']]
            write_df(df, ['File Index', 'File Name', f'Size ({unit})'], self.csv_file_sizes)

        def write_worker_transfers_to_csv(events_dict, target_file):
            time_set = set()
            col_data = {}
            for wid, events in events_dict.items():
                df = pd.DataFrame(events, columns=['time', 'delta'])
                df = df.groupby('time', as_index=False)['delta'].sum()
                df['cumulative'] = df['delta'].cumsum().clip(lower=0)
                raw = df[['time', 'cumulative']].values.tolist()
                compressed = compress_time_based_critical_points(raw)
                key = f"{wid[0]}:{wid[1]}:{wid[2]}"
                col_data[key] = {t: v for t, v in compressed}
                time_set.update(col_data[key].keys())
            if not col_data:
                return
            times = sorted(time_set)
            rows = [{'Time (s)': t, **{k: d.get(t, float('nan')) for k, d in col_data.items()}} for t in times]
            self.write_df_to_csv(pd.DataFrame(rows), target_file, index=False)

        def write_worker_storage_consumption_to_csv(storage_dict, target_file, percentage=False):
            time_set = set()
            col_data = {}
            for wid, events in storage_dict.items():
                events.sort()
                storage = 0
                timeline = []
                last = None
                for t, delta in events:
                    if last == t:
                        storage += delta
                        timeline[-1][1] = storage
                    else:
                        storage += delta
                        timeline.append([t, storage])
                        last = t
                timeline = [[t, max(0.0, s)] for t, s in timeline]
                if percentage:
                    worker = self.workers.get(wid)
                    if not worker or worker.disk_mb <= 0:
                        continue
                    timeline = [[t, s / worker.disk_mb * 100] for t, s in timeline]
                compressed = compress_time_based_critical_points(timeline)
                key = f"{wid[0]}:{wid[1]}:{wid[2]}"
                col_data[key] = {t: v for t, v in compressed}
                time_set.update(col_data[key].keys())
            if not col_data:
                return
            times = sorted(time_set)
            rows = [{'Time (s)': t, **{k: d.get(t, float('nan')) for k, d in col_data.items()}} for t in times]
            self.write_df_to_csv(pd.DataFrame(rows), target_file, index=False)

        write_worker_transfers_to_csv(worker_transfer_events['incoming'], self.csv_file_worker_incoming_transfers)
        write_worker_transfers_to_csv(worker_transfer_events['outgoing'], self.csv_file_worker_outgoing_transfers)
        write_worker_storage_consumption_to_csv(all_worker_storage, self.csv_file_worker_storage_consumption)
        write_worker_storage_consumption_to_csv(all_worker_storage, self.csv_file_worker_storage_consumption_percentage, percentage=True)

    def generate_task_execution_details_metrics(self):
        base_time = self.MIN_TIME
        rows = []

        for task in self.tasks.values():
            if not hasattr(task, 'core_id') or not task.core_id:
                continue
            if not task.worker_entry:
                continue

            worker = self.workers[task.worker_entry]
            worker_id = worker.id
            core_id = task.core_id[0]

            # Common task data
            task_data = {
                'task_id': task.task_id,
                'task_try_id': task.task_try_id,
                'worker_entry': f"{task.worker_entry[0]}:{task.worker_entry[1]}:{task.worker_entry[2]}",
                'worker_id': worker_id,
                'core_id': core_id,
                'is_recovery_task': getattr(task, 'is_recovery_task', False),
                'input_files': file_list_formatter(task.input_files) if task.input_files else '',
                'output_files': file_list_formatter(task.output_files) if task.output_files else '',
                'num_input_files': len(task.input_files) if task.input_files else 0,
                'num_output_files': len(task.output_files) if task.output_files else 0,
                'task_status': task.task_status,
                'category': getattr(task, 'category', ''),
                'when_ready': round(task.when_ready - base_time, 2) if task.when_ready else None,
                'when_running': round(task.when_running - base_time, 2) if task.when_running else None,
            }

            if task.task_status == 0:  # Successful task
                if not task.when_retrieved or getattr(task, 'is_library_task', False):
                    continue

                # Add successful task specific fields
                task_data.update({
                    'time_worker_start': round(task.time_worker_start - base_time, 2) if task.time_worker_start else None,
                    'time_worker_end': round(task.time_worker_end - base_time, 2) if task.time_worker_end else None,
                    'execution_time': round(task.time_worker_end - task.time_worker_start, 2) if task.time_worker_end and task.time_worker_start else None,
                    'when_waiting_retrieval': round(task.when_waiting_retrieval - base_time, 2) if task.when_waiting_retrieval else None,
                    'when_retrieved': round(task.when_retrieved - base_time, 2) if task.when_retrieved else None,
                    'when_done': round(task.when_done - base_time, 2) if task.when_done else None,
                    'record_type': 'successful_tasks',
                    'unsuccessful_checkbox_name': '',
                    'when_failure_happens': None,
                })
            else:  # Unsuccessful task
                task_data.update({
                    'time_worker_start': None,
                    'time_worker_end': None,
                    'when_waiting_retrieval': None,
                    'when_retrieved': None,
                    'when_failure_happens': round(task.when_failure_happens - base_time, 2) if task.when_failure_happens else None,
                    'execution_time': round(task.when_failure_happens - task.when_running, 2) if task.when_failure_happens and task.when_running else None,
                    'when_done': round(task.when_done - base_time, 2) if task.when_done else None,
                    'record_type': 'unsuccessful_tasks',
                    'unsuccessful_checkbox_name': TASK_STATUS_NAMES.get(task.task_status, 'unknown'),
                })

            rows.append(task_data)

        # Add worker data
        for worker in self.workers.values():
            if not getattr(worker, 'hash', None):
                continue

            # Ensure equal length lists for time_connected and time_disconnected
            time_disconnected = worker.time_disconnected[:]
            if len(time_disconnected) != len(worker.time_connected):
                time_disconnected.extend([self.MAX_TIME] * (len(worker.time_connected) - len(time_disconnected)))

            worker_data = {
                'task_id': pd.NA,
                'task_try_id': pd.NA,
                'worker_entry': f"{worker.ip}:{worker.port}:{worker.connect_id}",
                'worker_id': worker.id,
                'core_id': pd.NA,
                'is_recovery_task': pd.NA,
                'input_files': pd.NA,
                'output_files': pd.NA,
                'num_input_files': pd.NA,
                'num_output_files': pd.NA,
                'task_status': pd.NA,
                'category': pd.NA,
                'when_ready': pd.NA,
                'when_running': pd.NA,
                'time_worker_start': pd.NA,
                'time_worker_end': pd.NA,
                'execution_time': pd.NA,
                'when_waiting_retrieval': pd.NA,
                'when_retrieved': pd.NA,
                'when_failure_happens': pd.NA,
                'when_done': pd.NA,
                'record_type': 'worker',
                'unsuccessful_checkbox_name': pd.NA,
                'hash': worker.hash,
                'time_connected': [round(max(t - base_time, 0), 2) for t in worker.time_connected],
                'time_disconnected': [round(max(t - base_time, 0), 2) for t in time_disconnected],
                'cores': getattr(worker, 'cores', None),
                'memory_mb': getattr(worker, 'memory_mb', None),
                'disk_mb': getattr(worker, 'disk_mb', None),
                'gpus': getattr(worker, 'gpus', None)
            }
            rows.append(worker_data)
        
        # --- output ---

        if rows:
            df = pd.DataFrame(rows)
            
            # Define column order
            columns = [
                'record_type', 'task_id', 'task_try_id', 'worker_entry', 'worker_id', 'core_id',
                'is_recovery_task', 'input_files', 'output_files', 'num_input_files', 'num_output_files',
                'task_status', 'category', 'when_ready', 'when_running', 'time_worker_start',
                'time_worker_end', 'execution_time', 'when_waiting_retrieval', 'when_retrieved',
                'when_failure_happens', 'when_done', 'unsuccessful_checkbox_name', 'hash',
                'time_connected', 'time_disconnected', 'cores', 'memory_mb', 'disk_mb', 'gpus'
            ]
            
            # Reorder columns and fill missing ones with None
            for col in columns:
                if col not in df.columns:
                    df[col] = None
            df = df[columns]
            
            self.write_df_to_csv(df, self.csv_file_task_execution_details, index=False)
