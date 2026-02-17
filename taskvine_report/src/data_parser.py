import platform
import subprocess
from .worker_info import WorkerInfo
from .task_info import TaskInfo
from .file_info import FileInfo, IndexedTransferEvent, UnindexedTransferEvent
from .manager_info import ManagerInfo

import os
import math
import pandas as pd
import polars as pl
import traceback
from functools import lru_cache
from datetime import datetime
import numpy as np
from collections import defaultdict
import cloudpickle
from datetime import timezone, timedelta
import pytz
import platform
from taskvine_report.utils import *


# check VINE_PROTOCOL_VERSION in taskvine codebase to see if it matches this version
# if taskvine protocol version is changed, this file needs to be updated accordingly
VALID_VINE_PROTOCOL_VERSION = 14

def count_lines(file_name):
    if platform.system() in ["Linux", "Darwin"]:  # Linux or macOS
        try:
            return int(subprocess.check_output(["wc", "-l", file_name]).split()[0])
        except subprocess.CalledProcessError:
            pass

    with open(file_name, 'r', encoding='utf-8', errors='ignore') as f:
        return sum(1 for _ in f)


class DataParser:
    def __init__(self,
                 runtime_template, 
                 enablee_checkpoint_pkl_files=False, 
                 debug_mode=False):
        self.runtime_template = runtime_template
        self.enablee_checkpoint_pkl_files = enablee_checkpoint_pkl_files

        self.ip = None
        self.port = None
        self.transfer_port = None

        if not self.runtime_template:
            return

        # log files
        self.vine_logs_dir = os.path.join(self.runtime_template, 'vine-logs')
        self.json_files_dir = os.path.join(self.runtime_template, 'json-files')
        self.pkl_files_dir = os.path.join(self.runtime_template, 'pkl-files')
        ensure_dir(self.json_files_dir, replace=False)
        ensure_dir(self.pkl_files_dir, replace=False)

        self.debug = os.path.join(self.vine_logs_dir, 'debug')
        self.transactions = os.path.join(self.vine_logs_dir, 'transactions')   # not necessary
        self.taskgraph = os.path.join(self.vine_logs_dir, 'taskgraph')         # not necessary
        for file_path in [self.debug]:
            if not os.path.exists(file_path):
                raise ValueError(f"file {file_path} does not exist")

        # these are the main files for data analysis
        self.pkl_file_names = ['workers.pkl', 'files.pkl', 'tasks.pkl', 'manager.pkl', 'subgraphs.pkl']
        self.pkl_files = []
        for pkl_file_name in self.pkl_file_names:
            self.pkl_files.append(os.path.join(self.pkl_files_dir, pkl_file_name))

        # metadata pkl file
        self.pkl_file_metadata = os.path.join(self.pkl_files_dir, 'metadata.pkl')

        self.manager = ManagerInfo()

        # tasks
        self.tasks = {}        # key: (task_id, task_try_id), value: TaskInfo
        self.current_try_id = defaultdict(int)   # key: task_id, value: task_try_id

        # workers
        self.workers = {}      # key: (ip, port, connect_id), value: WorkerInfo
        self.current_worker_connect_id = defaultdict(int)  # key: (ip, port), value: connect_id
        self.map_ip_and_transfer_port_to_worker_port = {}  # key: (ip, transfer_port), value: WorkerInfo

        # files
        self.files = {}      # key: file_name, value: FileInfo

        # subgraphs
        self.subgraphs = {}   # key: subgraph_id, value: set()

        # status
        self.debug_mode = debug_mode
        self.receiving_resources_from_worker = None
        self.sending_task = None
        self.mini_task_transferring = None
        self.debug_current_line = None
        self.debug_current_parts = None
        self.debug_current_timestamp = None
        self._init_debug_handlers()
        self.sending_task_to_worker_entry = None

    def _init_debug_handlers(self):
        def H(name, cond, action):
            action.__name__ = name
            return (cond, action)

        self.debug_handlers = [

            H("busy_on",
            lambda l, p, ctx: "busy on" in l,
            lambda l, p, ctx: ctx._handle_debug_line_busy_on()),

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

            H("receive_worker_info",
            lambda l, p, ctx: " info " in l,
            lambda l, p, ctx: ctx._handle_debug_line_receive_worker_info()),

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

            H("put_file",
            lambda l, p, ctx: "put" in p,
            lambda l, p, ctx: ctx._handle_debug_line_put_file()),

            H("failed_to_send_task",
            lambda l, p, ctx: "Failed to send task" in l,
            lambda l, p, ctx: ctx._handle_debug_line_failed_to_send_task()),

            H("transfer_port",
            lambda l, p, ctx: "transfer-port" in p,
            lambda l, p, ctx: ctx._handle_debug_line_get_worker_transfer_port()),

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

            H("kill_task",
            lambda l, p, ctx: " kill " in l,
            lambda l, p, ctx: ctx._handle_debug_line_kill_task()),

            H("added_dependency",
            lambda l, p, ctx: "added dependency" in l,
            lambda l, p, ctx: None),

        ]
        self.debug_handler_profiling = defaultdict(lambda: {"hits": 0})

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

    def count_elements_after_current_parts(self, item):
        return count_elements_after(item, self.debug_current_parts)

    def set_time_zone(self, debug_file_path=None):
        if debug_file_path is None:
            debug_file_path = self.debug
            
        mgr_start_datestring = None
        mgr_start_timestamp = None

        # read the first line containing "listening on port" in debug file
        with open(debug_file_path, 'r') as file:
            for line in file:
                if "listening on port" in line:
                    parts = line.strip().split()
                    mgr_start_datestring = f"{parts[0]} {parts[1]}"
                    mgr_start_datestring = datetime.strptime(mgr_start_datestring, "%Y/%m/%d %H:%M:%S.%f").replace(microsecond=0)
                    break

        # read the first line containing "MANAGER" and "START" in transactions file
        if os.path.exists(self.transactions):
            with open(self.transactions, 'r') as file:
                for line in file:
                    if line.startswith('#'):
                        continue
                    if "MANAGER" in line and "START" in line:
                        parts = line.strip().split()
                        mgr_start_timestamp = int(int(parts[0]) / 1e6)
                        break
        # otherwise, report the error
        else:
            raise ValueError("Could not find manager start timestamp in transactions file.")

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

        print(f"Set time zone to {self.manager.equivalent_tz} offset {self.manager.time_zone_offset_hours}")

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
        removed_idx = self.debug_current_parts.index("removed")
        ip, port = WorkerInfo.extract_ip_port_from_string(self.debug_current_parts[removed_idx - 1])
        worker = self.get_current_worker_by_ip_port(ip, port)
        if worker is None:
            raise ValueError(f"worker {ip}:{port} is not found")

        worker.add_disconnection(self.debug_current_timestamp)
        self.manager.update_when_last_worker_disconnect(self.debug_current_timestamp)

    def _handle_debug_line_put_file(self):
        timestamp = self.debug_current_timestamp
        parts = self.debug_current_parts

        put_idx = parts.index("put")
        file_name = parts[put_idx + 1]
        file_size_mb = int(parts[put_idx + 3]) / 2**20
        ip, port = WorkerInfo.extract_ip_port_from_string(parts[put_idx - 1])
        dest_worker_entry = self.get_current_worker_entry_by_ip_port(ip, port)
        worker = self.workers[dest_worker_entry]

        file = self.ensure_file_info_entry(file_name, file_size_mb, timestamp)
        new_transfer = UnindexedTransferEvent(file_name, dest_worker_entry, timestamp)
        file.unindexed_transfers[dest_worker_entry].append(new_transfer)

        worker.add_active_file_or_transfer(file_name)

    def _handle_debug_line_worker_received(self):
        # do not take the synchronous worker received message as if the file has been staged in,
        # a cache-update will follow later if the file is indeed staged in
        return

    def _handle_debug_line_failed_to_send_task(self):
        task_id = int(self.debug_current_parts[self.debug_current_parts.index("task") + 1])
        task_entry = (task_id, self.current_try_id[task_id])
        task = self.tasks[task_entry]
        task.set_task_status(self.debug_current_timestamp, 43 << 3)   # failed to dispatch

    def _handle_debug_line_puturl(self):
        parts = self.debug_current_parts
        timestamp = self.debug_current_timestamp

        if "Already at worker" in self.debug_current_line:
            return

        puturl_id = parts.index("puturl") if "puturl" in parts else parts.index("puturl_now")
        file_name = parts[puturl_id + 2]
        size_in_mb = int(parts[puturl_id + 4]) / 2**20
        transfer_id = parts[-1]

        # the file name is the name on the worker's side
        file = self.ensure_file_info_entry(file_name, size_in_mb, timestamp)

        # the destination is definitely an ip:port worker
        dest_ip, dest_port = WorkerInfo.extract_ip_port_from_string(parts[puturl_id - 1])
        dest_worker = self.get_current_worker_by_ip_port(dest_ip, dest_port)
        assert dest_worker is not None

        dest_worker_entry = (dest_worker.ip, dest_worker.port, dest_worker.connect_id)
        # the source can be a url or an ip:port
        source = parts[puturl_id + 1]
        if source.startswith('https://') or source.startswith('file://'):
            file.indexed_transfers[transfer_id] = IndexedTransferEvent(file_name, dest_worker_entry, timestamp, transfer_id, source)
        elif source.startswith('workerip://'):
            source_ip, source_transfer_port = WorkerInfo.extract_ip_port_from_string(source)
            source_worker_port = self.map_ip_and_transfer_port_to_worker_port[(source_ip, source_transfer_port)]
            source_worker_entry = self.get_current_worker_entry_by_ip_port(source_ip, source_worker_port)
            assert source_worker_entry is not None
            file.indexed_transfers[transfer_id] = IndexedTransferEvent(file_name, dest_worker_entry, timestamp, transfer_id, source_worker_entry)
        else:
            raise ValueError(f"unrecognized source: {source}, line: {self.debug_current_line}")
        
        dest_worker.add_active_file_or_transfer(file_name)

    def _handle_debug_line_kill_task(self):
        assert self.count_elements_after_current_parts("kill") == 1
        task_id = int(self.debug_current_parts[self.debug_current_parts.index("kill") + 1])
        task_entry = (task_id, self.current_try_id[task_id])
        task = self.tasks[task_entry]
        # note that the task may not be committed (Failed to send task is followed)
        if task.worker_entry:
            worker = self.workers[task.worker_entry]
            worker.reap_task(task)

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

    def _handle_debug_line_receive_worker_info(self):
        parts = self.debug_current_parts
        info_idx = parts.index("info")
        ip, port = WorkerInfo.extract_ip_port_from_string(parts[info_idx - 1])
        worker = self.get_current_worker_by_ip_port(ip, port)
        if "worker-id" in parts:
            worker.set_hash(parts[info_idx + 2])
            worker.set_machine_name(parts[info_idx - 2])
        elif "tasks_running" in parts:
            pass
        elif "worker-end-time" in parts:
            pass
        elif "from-factory" in parts:
            pass
        else:
            pass

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
            self._match_sending_task_to_worker_entry(task, timestamp, True)
            return

        task_entry = (task_id, self.current_try_id[task_id])
        task = self.tasks[task_entry]

        if "READY (1) to RUNNING (2)" in line:                  # as expected
            # it could be that the task related info was unable to be sent (also comes with a "failed to send" message)
            # in this case, even the state is switched to running, there is no worker info
            if self.manager.when_first_task_start_commit is None:
                self.manager.set_when_first_task_start_commit(timestamp)

            self._match_sending_task_to_worker_entry(task, timestamp, False)

            if not task.worker_entry:
                return
            else:
                # update the coremap
                worker = self.workers[task.worker_entry]
                task.committed_worker_hash = worker.hash
                task.worker_id = worker.id
                core_id = worker.run_task(task)
                if core_id == -1:
                    print(f"Warning: worker {task.worker_entry} has no enough cores to run task {task_id}")
                    print(f"current running tasks: {worker.tasks_running}")
                # check if this is the first try
                if task_id not in self.current_try_id:
                    self.current_try_id[task_id] = 1
        elif "RUNNING (2) to RUNNING (2)" in line:
            raise ValueError(f"task {task_id} state change: from RUNNING (2) to RUNNING (2)")
        elif "RETRIEVED (4) to RUNNING (2)" in line:
            print(f"Warning: task {task_id} state change: from RETRIEVED (4) to RUNNING (2)")
            pass
        elif "RUNNING (2) to DONE (5)" in line:
            print(f"Warning: task {task_id} state change: from RUNNING (2) to DONE (5)")
            pass
        elif "DONE (5) to WAITING_RETRIEVAL (3)" in line:
            print(f"Warning: task {task_id} state change: from DONE (5) to WAITING_RETRIEVAL (3)")
            pass
        elif "READY (1) to RETRIEVED (4)" in line:
            # this can happen such as inputs missing
            task.set_when_retrieved(timestamp)
            task.set_task_status(timestamp, 1)
        elif "RUNNING (2) to WAITING_RETRIEVAL (3)" in line:    # as expected
            task.set_when_waiting_retrieval(timestamp)
            # update the coremap
            if not task.worker_entry:
                raise ValueError(f"task {task_id} has no worker entry")
            worker = self.workers[task.worker_entry]
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
            if task.worker_entry:
                worker = self.workers[task.worker_entry]
                worker.tasks_failed.append(task)
            # we need to set the task status if it was not set yet
            if task.task_status is None:
                # if it was committed to a worker
                if task.worker_entry:
                    worker = self.workers[task.worker_entry]
                    # it could be that the worker disconnected
                    if len(worker.time_connected) == len(worker.time_disconnected):
                        task.set_task_status(worker.time_disconnected[-1], 15 << 3)
                    # it could be that its inputs are missing
                    elif task.input_files:
                        all_inputs_ready = True
                        for input_file in task.input_files:
                            if input_file not in worker.current_replicas:
                                all_inputs_ready = False
                        if not all_inputs_ready:
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
        cache_update_idx = parts.index("cache-update")
        file_name = parts[cache_update_idx + 1]
        size_in_mb = int(parts[cache_update_idx + 4]) / 2**20
        transfer_id = parts[cache_update_idx + 8]   # 'X' or a real id

        # note: the transfer may be from an unknown worker and the transfer id could be invalid
        if transfer_id != 'X' and file_name not in self.files:
            return

        # if this is a task-generated file, it is the first time the file is cached on this worker, otherwise we only update the stage in time
        file = self.ensure_file_info_entry(file_name, size_in_mb, timestamp)

        ip, port = WorkerInfo.extract_ip_port_from_string(parts[cache_update_idx - 1])
        worker_entry = self.get_current_worker_entry_by_ip_port(ip, port)
        # TODO: better handle a special case where the file is created by a previous manager
        if worker_entry is None:
            return
        worker = self.workers[worker_entry]
        
        file.cache_update(worker, timestamp, transfer_id)

    def _handle_debug_line_cache_invalid(self):
        parts = self.debug_current_parts
        timestamp = self.debug_current_timestamp

        cache_invalid_idx = parts.index("cache-invalid")
        file_name = parts[cache_invalid_idx + 1]
        if file_name not in self.files:
            # special case: this file was created by a previous manager
            return
        
        if count_elements_after("cache-invalid", parts) == 3:
            transfer_id = parts[cache_invalid_idx + 3]
        else:
            transfer_id = None

        ip, port = WorkerInfo.extract_ip_port_from_string(parts[cache_invalid_idx - 1])
        worker_entry = (ip, port, self.current_worker_connect_id[(ip, port)])
        worker = self.workers[worker_entry]

        file = self.files[file_name]
        file.cache_invalid(worker, timestamp, transfer_id)

    def _handle_debug_line_unlink(self):
        if "total time spent on" in self.debug_current_line:
            return
        
        parts = self.debug_current_parts
        timestamp = self.debug_current_timestamp

        unlink_id = parts.index("unlink")
        file_name = parts[unlink_id + 1]
        ip, port = WorkerInfo.extract_ip_port_from_string(parts[unlink_id - 1])
        worker_entry = self.get_current_worker_entry_by_ip_port(ip, port)
        assert worker_entry is not None
        worker = self.workers[worker_entry]

        file = self.files[file_name]
        file.unlink(worker, timestamp)

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

    def _handle_debug_line_stdout(self):
        parts = self.debug_current_parts
        if self.count_elements_after_current_parts("stdout") != 2:
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

        # format: Submitted recovery task xxx to re-create lost temporary file xxx.
        file_name = parts[-1].rstrip(".")
        # this filename must have been appeared before
        if file_name not in self.files:
            raise ValueError(f"file {file_name} not found in files")
        file = self.files[file_name]
        file.add_producer(task)
        task.add_output_file(file_name)

    def _handle_debug_line_listening_on_port(self):
        self.manager.set_time_start(self.debug_current_timestamp)

    def _handle_debug_line_busy_on(self):
        parts = self.debug_current_parts
        busy_idx = parts.index("busy")
        worker_ip, worker_port = WorkerInfo.extract_ip_port_from_string(parts[busy_idx - 1])
        worker_entry = self.get_current_worker_entry_by_ip_port(worker_ip, worker_port)
        if worker_entry is None:
            raise ValueError(f"worker {worker_ip}:{worker_port} is not found")
        self.sending_task_to_worker_entry = worker_entry

    def _handle_debug_line_send_task_to_worker(self):
        parts = self.debug_current_parts
        line = self.debug_current_line
        timestamp = self.debug_current_timestamp

        if not self.sending_task:
            task_idx = parts.index("task")
            if self.count_elements_after_current_parts("task") != 1 or "tx to" not in line:
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
                    try:
                        handler_fn(line, parts, self)
                    except Exception:
                        raise ValueError(f"Failed in handler {handler_fn.__name__}")
                    self.debug_handler_profiling[handler_fn]["hits"] += 1
                    return
        else:
            for cond_fn, handler_fn in self.debug_handlers:
                if cond_fn(line, parts, self):
                    try:
                        handler_fn(line, parts, self)
                    except Exception:
                        raise ValueError(f"Failed in handler {handler_fn.__name__}")
                    return

    def _clean_debug_file(self):
        result = subprocess.run(
            ["grep", "-n", "tcp: listening on port", self.debug],
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode != 0:
            return
            
        lines = result.stdout.strip().split('\n')
        # Filter out empty lines that might result from strip().split()
        lines = [line for line in lines if line]

        if len(lines) == 0:
            return

        last_match_line_num = int(lines[-1].split(':')[0])
        if len(lines) > 1:
            print(f"Found {len(lines)} entries in the debug file, only keeping the last one")

        debug_cleaned = os.path.join(self.vine_logs_dir, 'debug.cleaned')
        
        with open(self.debug, 'r', encoding='utf-8', errors='ignore') as input_file:
            lines_to_keep = []
            for i, line in enumerate(input_file, 1):
                if i >= last_match_line_num:
                    lines_to_keep.append(line)
        
        with open(debug_cleaned, 'w', encoding='utf-8') as output_file:
            output_file.writelines(lines_to_keep)

    def _match_sending_task_to_worker_entry(self, task, when_running, is_library_task):
        if self.sending_task_to_worker_entry is not None:
            task.set_worker_entry(self.sending_task_to_worker_entry)
            task.set_when_running(when_running)
            task.is_library_task = is_library_task
        else:
            # if no busy on was above then the task commission failed and sending task to worker entry is None
            task.set_task_status(when_running, 43 << 3)   # failed to dispatch
        self.sending_task_to_worker_entry = None

    def parse_debug(self):
        # Remove existing debug.cleaned file if exists
        debug_cleaned = os.path.join(self.vine_logs_dir, 'debug.cleaned')
        if os.path.exists(debug_cleaned):
            os.remove(debug_cleaned)
        
        try:
            self.set_time_zone()
        except Exception as e:
            self._clean_debug_file()
            self.set_time_zone(debug_cleaned)
        
        # Use cleaned file if exists, otherwise use original
        debug_file_to_use = debug_cleaned if os.path.exists(debug_cleaned) else self.debug
        
        total_lines = count_lines(debug_file_to_use)
        debug_file_size_mb = floor_decimal(os.path.getsize(debug_file_to_use) / 1024 / 1024, 2)
        unit, scale = get_size_unit_and_scale(debug_file_size_mb)
        debug_file_size_str = f"{floor_decimal(debug_file_size_mb * scale, 2)} {unit}"

        with create_progress_bar() as progress:
            task_id = progress.add_task(f"[green]Parsing debug ({debug_file_size_str})", total=total_lines)
            resort_debug_handlers_interval = 10000
            with open(debug_file_to_use, 'rb') as file:
                for i, raw_line in enumerate(file):
                    progress.update(task_id, advance=1)
                    if i % resort_debug_handlers_interval == 0:
                        self._resort_debug_handlers()
                    try:
                        self.debug_current_line = raw_line.decode('utf-8').strip()
                        self.debug_current_parts = self.debug_current_line.strip().split(" ")
                    except UnicodeDecodeError:
                        # this sometimes happens, especially when the manager handles a null pointer
                        continue
                    try:
                        datestring = self.debug_current_parts[0] + " " + self.debug_current_parts[1]
                        self.debug_current_timestamp = floor_decimal(self.datestring_to_timestamp(datestring), 2)
                    except Exception:
                        # this line does not start with a timestamp, which sometimes happens
                        continue
                    try:
                        self.parse_debug_line()
                    except Exception as e:
                        print(f"Error parsing line {i}: {self.debug_current_line}")
                        print(traceback.format_exc())
                        exit(1)

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
        # parse the debug file
        self.parse_debug()
        
        # postprocess the debug
        self.postprocess_debug()

        # checkpoint pkl files so that later analysis can be done without re-parsing the debug file
        if self.enablee_checkpoint_pkl_files:
            self.checkpoint_pkl_files()

    def checkpoint_pkl_files(self):
        with create_progress_bar() as progress:
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
        # filter files with no producers
        self.files = {k: v for k, v in self.files.items() if len(v.producers) > 0}
        # append file_idx
        for idx, file in enumerate(self.files.values(), start=1):
            file.file_idx = idx
            file.unlink_all(self.manager.time_end)

    def load_pkl_files(self):
        # manually load every thing like checkpoint_pkl_files, rathan than using self.pkl_files in a loop
        with create_progress_bar() as progress:
            pbar = progress.add_task(f"[green]Loading pkl files", total=5)

            progress.update(pbar, description=f"[green]Loading workers.pkl")
            with open(os.path.join(self.pkl_files_dir, 'workers.pkl'), 'rb') as f:
                self.workers = cloudpickle.load(f)
            progress.advance(pbar)
            
            progress.update(pbar, description=f"[green]Loading files.pkl")
            with open(os.path.join(self.pkl_files_dir, 'files.pkl'), 'rb') as f:
                self.files = cloudpickle.load(f)
            progress.advance(pbar)
            
            progress.update(pbar, description=f"[green]Loading tasks.pkl")
            with open(os.path.join(self.pkl_files_dir, 'tasks.pkl'), 'rb') as f:
                self.tasks = cloudpickle.load(f)
            progress.advance(pbar)
            
            progress.update(pbar, description=f"[green]Loading manager.pkl")
            with open(os.path.join(self.pkl_files_dir, 'manager.pkl'), 'rb') as f:
                self.manager = cloudpickle.load(f)
            progress.advance(pbar)

            progress.update(pbar, description=f"[green]Loading subgraphs.pkl")
            with open(os.path.join(self.pkl_files_dir, 'subgraphs.pkl'), 'rb') as f:
                self.subgraphs = cloudpickle.load(f)
            progress.advance(pbar)
