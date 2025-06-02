import platform
import subprocess
from .worker_info import WorkerInfo
from .task_info import TaskInfo
from .file_info import FileInfo
from .manager_info import ManagerInfo

import os
from functools import lru_cache
from datetime import datetime
import time
from tqdm import tqdm
from collections import defaultdict
import cloudpickle
from datetime import timezone, timedelta
import pytz
from .utils import floor_decimal, get_unit_and_scale_by_max_file_size_mb


def count_lines(file_name):
    if platform.system() in ["Linux", "Darwin"]:  # Linux or macOS
        try:
            return int(subprocess.check_output(["wc", "-l", file_name]).split()[0])
        except subprocess.CalledProcessError:
            pass

    with open(file_name, 'r', encoding='utf-8', errors='ignore') as f:
        return sum(1 for _ in f)


class DataParser:
    def __init__(self, runtime_template):
        self.runtime_template = runtime_template

        self.ip = None
        self.port = None
        self.transfer_port = None

        # log files
        self.vine_logs_dir = os.path.join(self.runtime_template, 'vine-logs')
        self.csv_files_dir = os.path.join(self.runtime_template, 'csv-files')
        self.json_files_dir = os.path.join(self.runtime_template, 'json-files')
        self.pkl_files_dir = os.path.join(self.runtime_template, 'pkl-files')
        self.svg_files_dir = os.path.join(self.runtime_template, 'svg-files')
        os.makedirs(self.csv_files_dir, exist_ok=True)
        os.makedirs(self.json_files_dir, exist_ok=True)
        os.makedirs(self.pkl_files_dir, exist_ok=True)
        os.makedirs(self.svg_files_dir, exist_ok=True)

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
        self.receiving_resources_from_worker = None
        self.sending_task = None
        self.mini_task_transferring = None
        self.sending_back = None

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

    def ensure_file_info_entry(self, file_name: str, size_mb: float):
        if file_name not in self.files:
            self.files[file_name] = FileInfo(file_name, size_mb)
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

    def parse_debug_line(self, line):
        parts = line.strip().split(" ")
        try:
            datestring = parts[0] + " " + parts[1]
            timestamp = self.datestring_to_timestamp(datestring)
            timestamp = floor_decimal(timestamp, 2)
        except Exception:
            # this line does not start with a timestamp, which sometimes happens
            return

        self.manager.update_current_max_time(timestamp)

        if "listening on port" in line:
            try:
                self.manager.set_time_start(timestamp)
            except Exception:
                print(line)
                exit(1)
            return

        if "worker" in parts and "connected" in parts:
            worker_idx = parts.index("worker")
            ip, port = WorkerInfo.extract_ip_port_from_string(parts[worker_idx + 1])
            self.add_new_worker(ip, port, timestamp)
            self.manager.set_when_first_worker_connect(timestamp)
            return

        if "info" in parts and "worker-id" in parts:
            info_idx = parts.index("info")
            ip, port = WorkerInfo.extract_ip_port_from_string(parts[info_idx - 1])
            worker = self.get_current_worker_by_ip_port(ip, port)
            worker.set_hash(parts[info_idx + 2])
            worker.set_machine_name(parts[info_idx - 2])
            return

        if "removed" in parts and "worker" in parts:
            release_idx = parts.index("removed")
            ip, port = WorkerInfo.extract_ip_port_from_string(parts[release_idx - 1])
            worker = self.get_current_worker_by_ip_port(ip, port)
            assert worker is not None

            worker.add_disconnection(timestamp)
            self.manager.update_when_last_worker_disconnect(timestamp)
            
            worker_entry = (worker.ip, worker.port, worker.connect_id)
            # files on the worker are removed
            for filename in worker.active_files_or_transfers:
                self.files[filename].prune_file_on_worker_entry(worker_entry, timestamp)
            return

        if "transfer-port" in parts:
            transfer_port_idx = parts.index("transfer-port")
            transfer_port = int(parts[transfer_port_idx + 1])
            ip, port = WorkerInfo.extract_ip_port_from_string(parts[transfer_port_idx - 1])
            worker = self.get_current_worker_by_ip_port(ip, port)
            worker.set_transfer_port(transfer_port)
            self.map_ip_and_transfer_port_to_worker_port[(ip, transfer_port)] = port
            return

        if "put" in parts:
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

            file = self.ensure_file_info_entry(file_name, file_size_mb)
            transfer = file.add_transfer('manager', worker_entry, 'manager_put', file_type, file_cache_level)
            transfer.start_stage_in(timestamp, "pending")
            assert worker_entry not in self.putting_transfers
            worker = self.workers[worker_entry]
            worker.add_active_file_or_transfer(file_name)

            self.putting_transfers[worker_entry] = transfer
            return

        if "received" in parts:
            dest_ip, dest_port = WorkerInfo.extract_ip_port_from_string(parts[parts.index("received") - 1])
            dest_worker_entry = self.get_current_worker_entry_by_ip_port(dest_ip, dest_port)
            # the worker must be in the putting_transfers
            assert dest_worker_entry is not None and dest_worker_entry in self.putting_transfers
            
            transfer = self.putting_transfers[dest_worker_entry]
            transfer.stage_in(timestamp, "worker_received")
            del self.putting_transfers[dest_worker_entry]
            return

        if "Failed to send task" in line:
            task_id = int(parts[parts.index("task") + 1])
            task_entry = (task_id, self.current_try_id[task_id])
            task = self.tasks[task_entry]
            task.set_task_status(timestamp, 4 << 3)
            return

        if "exhausted resources on" in line:
            exhausted_idx = parts.index("exhausted")
            task_id = int(parts[exhausted_idx - 1])
            worker_ip, worker_port = WorkerInfo.extract_ip_port_from_string(parts[exhausted_idx + 4])
            task = self.tasks[(task_id, self.current_try_id[task_id])]
            return

        if parts[-1] == "resources":
            resources_idx = parts.index("resources")
            ip, port = WorkerInfo.extract_ip_port_from_string(parts[resources_idx - 1])
            self.receiving_resources_from_worker = self.get_current_worker_by_ip_port(ip, port)
            return
        if self.receiving_resources_from_worker and "cores" in parts:
            self.receiving_resources_from_worker.set_cores(
                int(float(parts[parts.index("cores") + 1])))
            return
        if self.receiving_resources_from_worker and "memory" in parts:
            self.receiving_resources_from_worker.set_memory_mb(
                int(float(parts[parts.index("memory") + 1])))
            return
        if self.receiving_resources_from_worker and "disk" in parts:
            self.receiving_resources_from_worker.set_disk_mb(
                int(float(parts[parts.index("disk") + 1])))
            return
        if self.receiving_resources_from_worker and "gpus" in parts:
            self.receiving_resources_from_worker.set_gpus(
                int(float(parts[parts.index("gpus") + 1])))
            return
        if self.receiving_resources_from_worker and "end" in parts:
            self.receiving_resources_from_worker = None
            return

        if "puturl" in parts or "puturl_now" in parts:
            if "puturl" in parts:
                transfer_event = 'puturl'
            else:
                transfer_event = 'puturl_now'

            puturl_id = parts.index("puturl") if "puturl" in parts else parts.index("puturl_now")
            file_name = parts[puturl_id + 2]
            file_cache_level = int(parts[puturl_id + 3])
            size_in_mb = int(parts[puturl_id + 4]) / 2**20

            # the file name is the name on the worker's side
            file = self.ensure_file_info_entry(file_name, size_in_mb)

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
            return

        if "mini_task" in line:
            mini_task_idx = parts.index("mini_task")
            source = parts[mini_task_idx + 1]
            file_name = parts[mini_task_idx + 2]
            cache_level = int(parts[mini_task_idx + 3])
            file_size = int(parts[mini_task_idx + 4]) / 2**20
            dest_worker_ip, dest_worker_port = WorkerInfo.extract_ip_port_from_string(parts[mini_task_idx - 1])
            
            dest_worker_entry = self.get_current_worker_entry_by_ip_port(dest_worker_ip, dest_worker_port)
            assert dest_worker_entry is not None
            
            file = self.ensure_file_info_entry(file_name, file_size)
            transfer = file.add_transfer(source, dest_worker_entry, 'mini_task', 2, cache_level)
            transfer.start_stage_in(timestamp, "pending")
            dest_worker = self.workers[dest_worker_entry]
            dest_worker.add_active_file_or_transfer(file_name)
            return

        if "tx to" in line and "task" in parts and parts.index("task") + 2 == len(parts):
            task_idx = parts.index("task")
            task_id = int(parts[task_idx + 1])

            if (task_id, self.current_try_id[task_id]) not in self.tasks:
                # this is a library task
                self.current_try_id[task_id] += 1
                task = TaskInfo(task_id, self.current_try_id[task_id])
                task.set_when_ready(timestamp)
                task.is_library_task = True
                self.add_task(task)

            self.sending_task = self.tasks[(
                task_id, self.current_try_id[task_id])]

            try:
                worker_ip, worker_port = WorkerInfo.extract_ip_port_from_string(parts[task_idx - 1])
                worker_entry = (worker_ip, worker_port, self.current_worker_connect_id[(worker_ip, worker_port)])
                if not self.sending_task.worker_entry:
                    self.sending_task.set_worker_entry(worker_entry)
            except Exception:
                raise
            return
        if self.sending_task:
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
                file = self.ensure_file_info_entry(file_name, 0)
                if not file.is_consumer(self.sending_task):
                    file.add_consumer(self.sending_task)
                    self.sending_task.add_input_file(file_name)
            elif "outfile" in parts:
                file_name = parts[parts.index("outfile") + 1]
                file = self.ensure_file_info_entry(file_name, 0)
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
            return

        if "state change:" in line:
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
                        # if the task was not committed to a worker, we donot know the reason
                        task.set_task_status(timestamp, 4 << 3)

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
            return

        if "complete" in parts:
            complete_idx = parts.index("complete")
            ip, port = WorkerInfo.extract_ip_port_from_string(parts[complete_idx - 1])
            worker_entry = (ip, port, self.current_worker_connect_id[(ip, port)])
            worker = self.workers[worker_entry]
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

            return

        if "stdout" in parts and (parts.index("stdout") + 3 == len(parts)):
            stdout_idx = parts.index("stdout")
            task_id = int(parts[stdout_idx + 1])
            task_entry = (task_id, self.current_try_id[task_id])
            task = self.tasks[task_entry]
            stdout_size_mb = int(parts[stdout_idx + 2]) / 2**20
            task.set_stdout_size_mb(stdout_size_mb)
            return

        if "has a ready transfer source for all files" in line:
            # skip as of now
            return

        if "cache-update" in parts:
            # cache-update cachename, &type, &cache_level, &size, &mtime, &transfer_time, &start_time, id
            cache_update_id = parts.index("cache-update")
            file_name = parts[cache_update_id + 1]
            file_type = parts[cache_update_id + 2]
            file_cache_level = parts[cache_update_id + 3]
            size_in_mb = int(parts[cache_update_id + 4]) / 2**20
            # start_sending_time = int(parts[cache_update_id + 7]) / 1e6

            # if this is a task-generated file, it is the first time the file is cached on this worker, otherwise we only update the stage in time
            file = self.ensure_file_info_entry(file_name, size_in_mb)

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

            return

        if "cache-invalid" in parts:
            cache_invalid_id = parts.index("cache-invalid")
            file_name = parts[cache_invalid_id + 1]
            if file_name not in self.files:
                # special case: this file was created by a previous manager
                return
            ip, port = WorkerInfo.extract_ip_port_from_string(parts[cache_invalid_id - 1])
            worker_entry = (ip, port, self.current_worker_connect_id[(ip, port)])
            file = self.files[file_name]
            file.cache_invalid(worker_entry, timestamp)
            return

        if "unlink" in parts:
            unlink_id = parts.index("unlink")
            file_name = parts[unlink_id + 1]
            ip, port = WorkerInfo.extract_ip_port_from_string(parts[unlink_id - 1])
            worker_entry = self.get_current_worker_entry_by_ip_port(ip, port)
            assert worker_entry is not None

            file = self.files[file_name]
            file.unlink(worker_entry, timestamp)
            worker = self.workers[worker_entry]
            worker.remove_active_file_or_transfer(file_name)
            return

        if "Submitted recovery task" in line:
            task_id = int(parts[parts.index("task") + 1])
            task_try_id = self.current_try_id[task_id]
            task = self.tasks[(task_id, task_try_id)]
            task.is_recovery_task = True
            return

        if "exhausted" in parts and "resources" in parts:
            exhausted_idx = parts.index("exhausted")
            task_id = int(parts[exhausted_idx - 1])
            task_try_id = self.current_try_id[task_id]
            task = self.tasks[(task_id, task_try_id)]
            task.exhausted_resources = True

        # get an output file from a worker, one worker can only send one file back at a time
        if "sending back" in line:
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
            return
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
        if self.sending_back and "sent" in parts:
            send_idx = parts.index("sent")
            source_ip, source_port = WorkerInfo.extract_ip_port_from_string(parts[send_idx - 1])
            source_worker_entry = self.get_current_worker_entry_by_ip_port(source_ip, source_port)
            assert source_worker_entry is not None
            assert source_worker_entry in self.sending_back_transfers

            transfer = self.sending_back_transfers[source_worker_entry]
            transfer.stage_in(timestamp, "manager_received")
            del self.sending_back_transfers[source_worker_entry]
            self.sending_back = False
            return

        if "manager end" in line:
            self.manager.set_time_end(timestamp)

        if "calculated penalty for file" in line:
            file_idx = parts.index("file")
            file_name = parts[file_idx + 1][:-1]
            penalty = float(parts[file_idx + 2])
            self.files[file_name].set_penalty(penalty)
            return

        if "designated as the PBB (checkpoint) worker" in line:
            designated_idx = parts.index("designated")
            ip, port = WorkerInfo.extract_ip_port_from_string(
                parts[designated_idx - 1])
            worker_entry = (ip, port, self.current_worker_connect_id[(ip, port)])
            worker = self.workers[worker_entry]
            worker.set_checkpoint_worker()
            return

        if "Removing instances of worker" in line:
            pass

        if "Checkpoint queue processing time" in line:
            time_idx = parts.index("time:")
            time_us = float(parts[time_idx + 1])
            self.manager.aggregate_checkpoint_processing_time(time_us)
            return

    def parse_debug(self):
        time_start = time.time()

        self.current_try_id = defaultdict(int)
        total_lines = count_lines(self.debug)
        debug_file_size_mb = floor_decimal(os.path.getsize(self.debug) / 1024 / 1024, 2)
        unit, scale = get_unit_and_scale_by_max_file_size_mb(debug_file_size_mb)

        print(f"Debug file size: {floor_decimal(debug_file_size_mb * scale, 2)} {unit}")
        with open(self.debug, 'rb') as file:
            pbar = tqdm(total=total_lines, desc="Parsing debug")
            for raw_line in file:
                pbar.update(1)
                try:
                    line = raw_line.decode('utf-8').strip()
                    self.parse_debug_line(line)
                except UnicodeDecodeError:
                    print(f"Error decoding line to utf-8: {raw_line}")
                except Exception as e:
                    print(f"Error parsing line: {line}")
                    raise e

            pbar.close()

        time_end = time.time()
        print(f"Parsing debug took {round(time_end - time_start, 4)} seconds")

        self.postprocess_debug()
        self.checkpoint_debug()

    def parse_logs(self):
        self.set_time_zone()

        self.parse_debug()

    def generate_subgraphs(self):
        time_start = time.time()
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

        pbar = tqdm(self.files.values(), desc="Parsing subgraphs")
        for file in pbar:
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

        time_end = time.time()
        print(f"Parsing subgraphs took {round(time_end - time_start, 4)} seconds")

        self.checkpoint_subgraphs()

    def checkpoint_debug(self):
        time_start = time.time()
        with open(os.path.join(self.pkl_files_dir, 'workers.pkl'), 'wb') as f:
            cloudpickle.dump(self.workers, f)
        time_end = time.time()
        print(f"Checkpointing workers.pkl took {round(time_end - time_start, 4)} seconds")
        time_start = time.time()
        with open(os.path.join(self.pkl_files_dir, 'files.pkl'), 'wb') as f:
            cloudpickle.dump(self.files, f)
        time_end = time.time()
        print(f"Checkpointing files.pkl took {round(time_end - time_start, 4)} seconds")
        time_start = time.time()
        with open(os.path.join(self.pkl_files_dir, 'tasks.pkl'), 'wb') as f:
            cloudpickle.dump(self.tasks, f)
        time_end = time.time()
        print(f"Checkpointing tasks.pkl took {round(time_end - time_start, 4)} seconds")
        time_start = time.time()
        with open(os.path.join(self.pkl_files_dir, 'manager.pkl'), 'wb') as f:
            cloudpickle.dump(self.manager, f)
        time_end = time.time()
        print(f"Checkpointing manager.pkl took {round(time_end - time_start, 4)} seconds")

    def postprocess_debug(self):
        time_start = time.time()
        # some post-processing in case the manager does not exit normally or has not finished yet
        # if the manager has not finished yet, we do something to set up the None values to make the plotting tool work
        # 1. if the manager's time_end is None, we set it to the current timestamp
        if self.manager.time_end is None:
            print(f"Manager didn't exit normally, setting manager time_end to {self.manager.current_max_time}")
            self.manager.set_time_end(self.manager.current_max_time)

        # post-processing for tasks
        for task in self.tasks.values():
            # if a task's status is None, we set it to 4 << 3, which means the task failed but not yet reported
            if task.task_status is None:
                task.set_task_status(self.manager.current_max_time, 4 << 3)
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
                raise ValueError(f"task {task.task_id} when_retrieved is smaller than time_worker_end: {task.time_worker_end} - {task.when_retrieved}")
        # post-processing for workers
        for worker in self.workers.values():
            # for workers, check if the time_disconnected is larger than the time_connected
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
        time_end = time.time()
        print(
            f"Postprocessing debug took {round(time_end - time_start, 4)} seconds")

    def restore_debug(self):
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

    def checkpoint_subgraphs(self):
        time_start = time.time()
        with open(os.path.join(self.pkl_files_dir, 'subgraphs.pkl'), 'wb') as f:
            cloudpickle.dump(self.subgraphs, f)
        time_end = time.time()
        print(f"Checkpointing subgraphs.pkl took {round(time_end - time_start, 4)} seconds")

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
