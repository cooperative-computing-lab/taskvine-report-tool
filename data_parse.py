from worker_info import WorkerInfo
from task_info import TaskInfo
from file_info import FileInfo
from manager_info import ManagerInfo

import os
import json
from datetime import datetime
import time
from tqdm import tqdm
from collections import defaultdict
import cloudpickle
from datetime import datetime, timezone, timedelta
import pytz
from decimal import Decimal, ROUND_FLOOR


def floor_decimal(number, decimal_places):
    num = Decimal(str(number))
    quantizer = Decimal(f"1e-{decimal_places}")
    return num.quantize(quantizer, rounding=ROUND_FLOOR)


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
        os.makedirs(self.csv_files_dir, exist_ok=True)
        os.makedirs(self.json_files_dir, exist_ok=True)
        os.makedirs(self.pkl_files_dir, exist_ok=True)

        self.debug = os.path.join(self.vine_logs_dir, 'debug')
        self.transactions = os.path.join(self.vine_logs_dir, 'transactions')
        self.taskgraph = os.path.join(self.vine_logs_dir, 'taskgraph')
        self.daskvine_log = os.path.join(self.vine_logs_dir, 'daskvine.log')

        # output csv files
        self.manager = ManagerInfo()

        # tasks
        self.tasks = {}        # key: (task_id, task_try_id), value: TaskInfo
        self.current_try_id = {}   # key: task_id, value: task_try_id

        # workers
        self.workers = {}      # key: (ip, port), value: WorkerInfo
        self.ip_transfer_port_to_worker = {}     # key: (ip, transfer_port), value: WorkerInfo
        self.max_id = 0             # starting from 1

        # files
        self.files = {}      # key: filename, value: FileInfo

        # time info
        self.set_time_zone()
        self.manager.time_zone_offset_hours = self.time_zone_offset_hours

    def worker_ip_port_to_hash(self, worker_ip: str, worker_port: int):
        return f"{worker_ip}:{worker_port}"

    def set_time_zone(self):
        mgr_start_datestring = None
        mgr_start_timestamp = None

        # read the first line containing "listening on port" in debug file
        with open(self.debug, 'r') as file:
            for line in file:
                if "listening on port" in line:
                    parts = line.strip().split(" ")
                    mgr_start_datestring = parts[0] + " " + parts[1]
                    mgr_start_datestring = datetime.strptime(mgr_start_datestring, "%Y/%m/%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M")
                    break

        # read the first line containing "MANAGER" and "START" in transactions file
        with open(self.transactions, 'r') as file:
            for line in file:
                if line.startswith('#'):
                    continue
                if "MANAGER" in line and "START" in line:
                    parts = line.strip().split(" ")
                    mgr_start_timestamp = int(parts[0])
                    mgr_start_timestamp = int(mgr_start_timestamp / 1e6)
                    break

        # calculate the time zone offset
        utc_datestring = datetime.fromtimestamp(mgr_start_timestamp, timezone.utc)
        for tz in pytz.all_timezones:
            tz_datestring = utc_datestring.replace(tzinfo=pytz.utc).astimezone(pytz.timezone(tz)).strftime('%Y-%m-%d %H:%M')
            if mgr_start_datestring == tz_datestring:
                self.time_zone_offset_hours = int(pytz.timezone(tz).utcoffset(datetime.now()).total_seconds() / 3600)
                break

    def datestring_to_timestamp(self, datestring):
        equivalent_tz = timezone(timedelta(hours=self.time_zone_offset_hours))
        equivalent_datestring = datetime.strptime(datestring, "%Y/%m/%d %H:%M:%S.%f").replace(tzinfo=equivalent_tz)
        unix_timestamp = float(equivalent_datestring.timestamp())

        return unix_timestamp
    
    def ensure_worker_entry(self, worker_ip: str, worker_port: int):
        worker_entry = (worker_ip, worker_port)
        if worker_entry not in self.workers:
            self.workers[worker_entry] = WorkerInfo(worker_ip, worker_port, self)
        return self.workers[worker_entry]
    
    def ensure_file_info_entry(self, filename: str, size_mb: float):
        if filename not in self.files:
            self.files[filename] = FileInfo(filename, size_mb)
        file = self.files[filename]
        if size_mb > 0:
            file.set_size_mb(size_mb)
        return file

    def add_task(self, task: TaskInfo):
        assert isinstance(task, TaskInfo)
        task_entry = (task.task_id, task.task_try_id)
        if task_entry in self.tasks:
            raise ValueError(f"task {task.task_id} already exists")
        self.tasks[task_entry] = task

    def add_worker(self, worker: WorkerInfo):
        assert isinstance(worker, WorkerInfo)
        worker_entry = (worker.ip, worker.port)
        if worker_entry in self.workers:
            raise ValueError(f"worker {worker.ip}:{worker.port} already exists")
        self.max_id += 1
        worker.id = self.max_id
        self.workers[worker_entry] = worker

    def add_ip_transfer_port_to_worker(self, ip: str, transfer_port: int, worker: WorkerInfo):
        # if there is an existing worker, it must had disconnected
        existing_worker = self.find_worker_by_ip_transfer_port(ip, transfer_port)
        if existing_worker:
            assert len(existing_worker.time_connected) == len(existing_worker.time_disconnected)
        self.ip_transfer_port_to_worker[(ip, transfer_port)] = worker

    def find_worker_by_ip_transfer_port(self, ip: str, transfer_port: int):
        if (ip, transfer_port) in self.ip_transfer_port_to_worker:
            return self.ip_transfer_port_to_worker[(ip, transfer_port)]
    
    def parse_transactions(self):
        # initialize the current_try_id for all tasks
        for task_id in self.current_try_id.keys():
            self.current_try_id[task_id] = 0

        total_lines = 0
        with open(self.transactions, 'r') as file:
            for line in file:
                total_lines += 1

        with open(self.transactions, 'r') as file:
            pbar = tqdm(total=total_lines, desc="parsing transactions")
            for line in file:
                pbar.update(1)
                if line.startswith('#'):
                    continue
                
                timestamp, _, event_type, obj_id, status, *info = line.split(maxsplit=5)
                try:
                    timestamp = floor_decimal(float(timestamp) / 1e6, 4)
                except ValueError:
                    continue

                info = info[0] if info else "{}"

                if event_type == 'TASK':
                    if status == 'READY':
                        task_id = int(obj_id)
                        self.current_try_id[task_id] += 1
                        assert (task_id, self.current_try_id[task_id]) in self.tasks
                        continue
                    task_id = int(obj_id)
                    task_entry = (task_id, self.current_try_id[task_id])

                    task = self.tasks[task_entry]
                    if status == 'RUNNING':
                        resources_allocated = json.loads(info.split(' ', 3)[-1])
                        task.time_commit_start = float(resources_allocated["time_commit_start"][0])
                        task.time_commit_end = float(resources_allocated["time_commit_end"][0])
                        self.manager.set_when_first_task_start_commit(task.time_commit_start)
                        continue
                    if status == 'WAITING_RETRIEVAL':
                        continue
                    if status == 'RETRIEVED':
                        continue
                    if status == 'DONE':
                        continue

                if event_type == 'WORKER':
                    pass
                
                if event_type == 'LIBRARY':
                    if status == 'SENT':
                        raise ValueError(f"LIBRARY SENT is not supported yet")
                    if status == 'STARTED':
                        raise ValueError(f"LIBRARY STARTED is not supported yet")          

    def parse_debug(self):
        self.current_try_id = defaultdict(int)

        total_lines = 0
        with open(self.debug, 'r') as file:
            for line in file:
                total_lines += 1

        # put a file on a worker
        putting_transfer_event = None
        getting_transfer_event = None

        # receiving resources info from a worker
        receiving_resources_from_worker = None

        # send task info to a worker
        sending_task_id = None

        with open(self.debug, 'r') as file:
            pbar = tqdm(total=total_lines, desc="parsing debug")
            for line in file:
                pbar.update(1)
                parts = line.strip().split(" ")
                datestring = parts[0] + " " + parts[1]
                timestamp = self.datestring_to_timestamp(datestring)
                timestamp = floor_decimal(timestamp, 2)

                if "listening on port" in line:
                    self.manager.set_time_start(timestamp)
                    continue

                if "worker" in parts and "connected" in parts:
                    # this is the first time a worker is connected to the manager
                    worker_idx = parts.index("worker")
                    ip, port = WorkerInfo.extract_ip_port_from_string(parts[worker_idx + 1])
                    worker = WorkerInfo(ip, port)
                    worker.add_connection(timestamp)
                    self.add_worker(worker)
                    self.manager.set_when_first_worker_connect(timestamp)
                    continue

                if "info" in parts and "worker-id" in parts:
                    info_idx = parts.index("info")
                    ip, port =  WorkerInfo.extract_ip_port_from_string(parts[info_idx - 1])
                    worker = self.workers[(ip, port)]
                    worker.set_hash(parts[info_idx + 2])
                    worker.set_machine_name(parts[info_idx - 2])
                    continue

                if "removed" in parts:
                    release_idx = parts.index("removed")
                    ip, port = WorkerInfo.extract_ip_port_from_string(parts[release_idx - 1])
                    worker = self.workers[(ip, port)]
                    worker.add_disconnection(timestamp)

                    for file in self.files.values():
                        file.worker_removed((ip, port), timestamp)
                    self.manager.update_when_last_worker_disconnect(timestamp)
                    continue

                if "transfer-port" in parts:
                    transfer_port_idx = parts.index("transfer-port")
                    transfer_port = int(parts[transfer_port_idx + 1])
                    ip, port = WorkerInfo.extract_ip_port_from_string(parts[transfer_port_idx - 1])
                    worker = self.workers[(ip, port)]
                    worker.set_transfer_port(transfer_port)
                    self.add_ip_transfer_port_to_worker(ip, transfer_port, worker)
                    continue

                if "put" in parts:
                    put_idx = parts.index("put")
                    ip, port = WorkerInfo.extract_ip_port_from_string(parts[put_idx - 1])
                    worker = self.workers[(ip, port)]
                    filename = parts[put_idx + 1]

                    file_cache_level = parts[put_idx + 2]
                    file_size_mb = int(parts[put_idx + 3]) / 2**20

                    if filename.startswith('buffer'):
                        file_type = 4
                    elif filename.startswith('file'):
                        file_type = 1
                    else:
                        raise ValueError(f"pending file type: {filename}")

                    file = self.ensure_file_info_entry(filename, file_size_mb)
                    putting_transfer_event = file.add_transfer('manager', (worker.ip, worker.port), 'manager_put', file_type, file_cache_level)
                    putting_transfer_event.start_stage_in(timestamp, "pending")
                    continue
                if putting_transfer_event and "file" in parts:
                    continue
                if putting_transfer_event and "received" in parts:
                    putting_transfer_event.stage_in(timestamp, "worker_received")
                    putting_transfer_event = None
                    continue
                if putting_transfer_event:
                    continue

                if "exhausted resources on" in line:
                    exhausted_idx = parts.index("exhausted")
                    task_id = int(parts[exhausted_idx - 1])
                    worker_ip, worker_port = WorkerInfo.extract_ip_port_from_string(parts[exhausted_idx + 4])
                    task = self.tasks[(task_id, self.current_try_id[task_id])]
                    continue

                if "resources" in parts and line.endswith("resources\n"):
                    resources_idx = parts.index("resources")
                    worker_ip, worker_port = WorkerInfo.extract_ip_port_from_string(parts[resources_idx - 1])
                    receiving_resources_from_worker = self.workers[(worker_ip, worker_port)]
                    continue
                if receiving_resources_from_worker and "cores" in parts:
                    receiving_resources_from_worker.set_cores(int(float(parts[parts.index("cores") + 1])))
                    continue
                if receiving_resources_from_worker and "memory" in parts:
                    receiving_resources_from_worker.set_memory_mb(int(float(parts[parts.index("memory") + 1])))
                    continue
                if receiving_resources_from_worker and "disk" in parts:
                    receiving_resources_from_worker.set_disk_mb(int(float(parts[parts.index("disk") + 1])))
                    continue
                if receiving_resources_from_worker and "gpus" in parts:
                    receiving_resources_from_worker.set_gpus(int(float(parts[parts.index("gpus") + 1])))
                    continue
                if receiving_resources_from_worker and "end" in parts:
                    receiving_resources_from_worker = None
                    continue

                if "puturl" in parts or "puturl_now" in parts:
                    puturl_id = parts.index("puturl") if "puturl" in parts else parts.index("puturl_now")
                    filename = parts[puturl_id + 2]
                    file_cache_level = int(parts[puturl_id + 3])
                    size_in_mb = int(parts[puturl_id + 4]) / 2**20
                    
                    source_ip, source_transfer_port = WorkerInfo.extract_ip_port_from_string(parts[puturl_id + 1])
                    source_worker = self.find_worker_by_ip_transfer_port(source_ip, source_transfer_port)
                    dest_ip, dest_port = WorkerInfo.extract_ip_port_from_string(parts[puturl_id - 1])
                    dest_worker = self.workers[(dest_ip, dest_port)]

                    if "puturl" in parts:
                        transfer_event = 'puturl'
                    else:
                        transfer_event = 'puturl_now'

                    file = self.ensure_file_info_entry(filename, size_in_mb)
                    transfer = file.add_transfer((source_worker.ip, source_worker.port), (dest_worker.ip, dest_worker.port), transfer_event, 2, file_cache_level)
                    transfer.start_stage_in(timestamp, "pending")
                    continue

                if "tx to" in line and "task" in parts:
                    task_idx = parts.index("task")
                    sending_task_id = int(parts[task_idx + 1])
                    sending_task_try_id = self.current_try_id[sending_task_id]
                    sending_task = self.tasks[(sending_task_id, sending_task_try_id)]
                    worker_ip, worker_port = WorkerInfo.extract_ip_port_from_string(parts[task_idx - 1])
                    sending_task.set_worker_ip_port(worker_ip, worker_port)
                    continue
                if sending_task_id:
                    task = self.tasks[(sending_task_id, self.current_try_id[sending_task_id])]
                    if "end" in parts:
                        sending_task_id = None
                    elif "cores" in parts:
                        task.set_cores_requested(int(float(parts[parts.index("cores") + 1])))
                    elif "gpus" in parts:
                        task.set_gpus_requested(int(float(parts[parts.index("gpus") + 1])))
                    elif "memory" in parts:
                        task.set_memory_requested_mb(int(float(parts[parts.index("memory") + 1])))
                    elif "disk" in parts:
                        task.set_disk_requested_mb(int(float(parts[parts.index("disk") + 1])))
                    elif "category" in parts:
                        task.set_category(parts[parts.index("category") + 1])
                    elif "infile" in parts:
                        filename = parts[parts.index("infile") + 1]
                        file = self.ensure_file_info_entry(filename, 0)
                        file.add_consumer(task.task_id)
                        task.add_input_file(filename)
                    elif "outfile" in parts:
                        filename = parts[parts.index("outfile") + 1]
                        file = self.ensure_file_info_entry(filename, 0)
                        file.add_producer(task.task_id)
                        task.add_output_file(filename)
                    continue

                if "state change:" in line:
                    task_id = int(parts[parts.index("Task") + 1])
                    if "INITIAL (0) to READY (1)" in line:                  # a brand new task
                        assert task_id not in self.current_try_id
                        # new task entry
                        self.current_try_id[task_id] += 1
                        task = TaskInfo(task_id, self.current_try_id[task_id], self)
                        task.set_when_ready(timestamp)
                        self.add_task(task)
                        continue

                    task_entry = (task_id, self.current_try_id[task_id])
                    task = self.tasks[task_entry]
                    if "READY (1) to RUNNING (2)" in line:                  # as expected 
                        # update the coremap
                        worker_entry = (task.worker_ip, task.worker_port)
                        worker = self.workers[worker_entry]
                        task.set_when_running(timestamp)
                        task.committed_worker_hash = worker.hash
                        task.worker_id = worker.id
                        worker.run_task(task)
                    elif "RUNNING (2) to WAITING_RETRIEVAL (3)" in line:    # as expected
                        task.set_when_waiting_retrieval(timestamp)
                        # update the coremap
                        worker_entry = (task.worker_ip, task.worker_port)
                        worker = self.workers[worker_entry]
                        worker.reap_task(task)
                    elif "WAITING_RETRIEVAL (3) to RETRIEVED (4)" in line:  # as expected
                        task.set_when_retrieved(timestamp)
                    elif "RETRIEVED (4) to DONE (5)" in line:               # as expected
                        worker_entry = (task.worker_ip, task.worker_port)
                        worker = self.workers[worker_entry]
                        task.set_when_done(timestamp)
                        self.manager.set_when_last_task_done(timestamp)
                        worker.tasks_completed.append(task)
                    elif "WAITING_RETRIEVAL (3) to READY (1)" in line or \
                         "RUNNING (2) to READY (1)" in line:                # task failure
                        # new task entry
                        self.current_try_id[task_id] += 1
                        new_task = TaskInfo(task_id, self.current_try_id[task_id], self)
                        # skip if the task status was set by a complete message
                        if not task.task_status:
                            task.set_when_failure_happens(timestamp)
                            worker_entry = (task.worker_ip, task.worker_port)
                            worker = self.workers[worker_entry]
                            # it is that the worker disconnected
                            if len(worker.time_connected) == len(worker.time_disconnected):
                                task.set_task_status(15 << 3)
                            # otherwise, we do not know the reason
                            else:
                                task.set_task_status(4 << 3)
                        new_task.set_when_ready(timestamp)
                        self.add_task(new_task)
                        # update the worker's tasks_failed
                        worker_entry = (task.worker_ip, task.worker_port)
                        worker = self.workers[worker_entry]
                        worker.tasks_failed.append(task)
                        worker.reap_task(task)
                    else:
                        raise ValueError(f"pending state change: {line}")
                    continue

                if "complete" in parts:
                    complete_idx = parts.index("complete")
                    worker_ip, worker_port = WorkerInfo.extract_ip_port_from_string(parts[complete_idx - 1])
                    worker = self.workers[(worker_ip, worker_port)]
                    task_status = int(parts[complete_idx + 1])
                    exit_status = int(parts[complete_idx + 2])
                    output_length = int(parts[complete_idx + 3])
                    bytes_sent = int(parts[complete_idx + 4])
                    time_worker_start = round(float(parts[complete_idx + 5]) / 1e6, 2)
                    time_worker_end = round(float(parts[complete_idx + 6]) / 1e6, 2)
                    sandbox_used = None
                    try:
                        task_id = int(parts[complete_idx + 8])
                        sandbox_used = int(parts[complete_idx + 7])
                    except:
                        task_id = int(parts[complete_idx + 7])
                        
                    task_entry = (task_id, self.current_try_id[task_id])
                    task = self.tasks[task_entry]
                    
                    task.set_task_status(task_status)
                    if task_status != 0:
                        task.set_when_failure_happens(timestamp)
                    
                    task.set_exit_status(exit_status)
                    task.set_output_length(output_length)
                    task.set_bytes_sent(bytes_sent)

                    task.set_time_worker_start(time_worker_start)
                    task.set_time_worker_end(time_worker_end)
                    task.set_sandbox_used(sandbox_used)
                    continue

                if "stdout" in parts and (parts.index("stdout") + 3 == len(parts)):
                    stdout_idx = parts.index("stdout")
                    task_id = int(parts[stdout_idx + 1])
                    task_entry = (task_id, self.current_try_id[task_id])
                    task = self.tasks[task_entry]
                    stdout_size_mb = int(parts[stdout_idx + 2]) / 2**20
                    task.set_stdout_size_mb(stdout_size_mb)
                    continue

                if "has" in parts and "a" in parts and "ready" in parts and "transfer" in parts:
                    has_idx = parts.index("has")
                    task_id = int(parts[has_idx - 1])
                    if task_id not in self.current_try_id:
                        self.current_try_id[task_id] = 1    # this is the first try

                    task_entry = (task_id, self.current_try_id[task_id])
                    self.tasks[task_entry].when_input_transfer_ready = timestamp
                    continue

                if "cache-update" in parts:
                    # cache-update cachename, &type, &cache_level, &size, &mtime, &transfer_time, &start_time, id
                    cache_update_id = parts.index("cache-update")
                    ip, port = WorkerInfo.extract_ip_port_from_string(parts[cache_update_id - 1])
                    worker = self.workers[(ip, port)]

                    filename = parts[cache_update_id + 1]
                    file_type = parts[cache_update_id + 2]
                    file_cache_level = parts[cache_update_id + 3]
                    size_in_mb = int(parts[cache_update_id + 4]) / 2**20
                    # start_sending_time = int(parts[cache_update_id + 7]) / 1e6

                    # if this is a task-generated file, it is the first time the file is cached on this worker, otherwise we only update the stage in time
                    file = self.ensure_file_info_entry(filename, size_in_mb)
                    # let the file handle the cache update
                    file.cache_update((worker.ip, worker.port), timestamp, file_type, file_cache_level)
                    continue
                    
                if "cache-invalid" in parts:
                    cache_invalid_id = parts.index("cache-invalid")
                    ip, port = WorkerInfo.extract_ip_port_from_string(parts[cache_invalid_id - 1])
                    worker = self.workers[(ip, port)]
                    filename = parts[cache_invalid_id + 1]

                    file = self.files[filename]
                    file.cache_invalid((ip, port), timestamp)
                    continue

                if "unlink" in parts:
                    unlink_id = parts.index("unlink")
                    filename = parts[unlink_id + 1]
                    ip, port = WorkerInfo.extract_ip_port_from_string(parts[unlink_id - 1])
                    worker = self.workers[(ip, port)]

                    file = self.files[filename]
                    file.unlink((ip, port), timestamp)
                    continue
                    
                if "Submitted recovery task" in line:
                    task_id = int(parts[parts.index("task") + 1])
                    task_try_id = self.current_try_id[task_id]
                    task = self.tasks[(task_id, task_try_id)]
                    task.is_recovery_task = True
                    continue

                if "exhausted" in parts and "resources" in parts:
                    exhausted_idx = parts.index("exhausted")
                    task_id = int(parts[exhausted_idx - 1])
                    task_try_id = self.current_try_id[task_id]
                    task = self.tasks[(task_id, task_try_id)]
                    task.exhausted_resources = True
                
                # get an output file from a worker
                if "sending back" in line:
                    assert getting_transfer_event is None
                    getting_transfer_event = True
                    continue
                if getting_transfer_event and "get" in parts:
                    continue
                if getting_transfer_event and "file" in parts and "Receiving" not in parts:
                    continue
                if getting_transfer_event and "Receiving file" in line:
                    file_idx = parts.index("file")
                    filename = parts[file_idx + 1]
                    file_size_mb = int(parts[parts.index("(size:") + 1]) / 2**20
                    source_ip, source_port = WorkerInfo.extract_ip_port_from_string(parts[parts.index("from") + 1])

                    file = self.ensure_file_info_entry(filename, file_size_mb)
                    getting_transfer_event = file.add_transfer((source_ip, source_port), 'manager', 'manager_get', 1, 1)
                    getting_transfer_event.start_stage_in(timestamp, "pending")
                    continue
                if getting_transfer_event and "sent" in parts:
                    getting_transfer_event.stage_in(timestamp, "manager_received")
                    getting_transfer_event = None
                    continue

                if "manager end" in line:
                    self.manager.set_time_end(timestamp)
                    for task in self.tasks.values():
                        # task was retrieved but not yet done
                        if task.when_done is None and task.when_retrieved is not None:
                            task.set_when_done(timestamp)
                        # task was not retrieved
                        elif task.when_retrieved is None:
                            if not task.when_failure_happens:
                                task.set_when_failure_happens(timestamp)
                        else:
                            pass

            pbar.close()

    def parse_logs(self):
        self.parse_debug()
        self.parse_transactions()

    def checkpoint(self):
        # save the workers, files, and tasks
        time_start = time.time()
        with open(os.path.join(self.pkl_files_dir, 'workers.pkl'), 'wb') as f:
            cloudpickle.dump(self.workers, f)
        with open(os.path.join(self.pkl_files_dir, 'files.pkl'), 'wb') as f:
            cloudpickle.dump(self.files, f)
        with open(os.path.join(self.pkl_files_dir, 'tasks.pkl'), 'wb') as f:
            cloudpickle.dump(self.tasks, f)
        with open(os.path.join(self.pkl_files_dir, 'manager.pkl'), 'wb') as f:
            cloudpickle.dump(self.manager, f)
        time_end = time.time()
        print(f"Checkpoint saved in {round(time_end - time_start, 4)} seconds")

    def restore_from_checkpoint(self):
        # restore the workers, files, and tasks
        time_start = time.time()
        with open(os.path.join(self.pkl_files_dir, 'workers.pkl'), 'rb') as f:
            self.workers = cloudpickle.load(f)
        with open(os.path.join(self.pkl_files_dir, 'files.pkl'), 'rb') as f:
            self.files = cloudpickle.load(f)
        with open(os.path.join(self.pkl_files_dir, 'tasks.pkl'), 'rb') as f:
            self.tasks = cloudpickle.load(f)
        with open(os.path.join(self.pkl_files_dir, 'manager.pkl'), 'rb') as f:
            self.manager = cloudpickle.load(f)
        time_end = time.time()
        print(f"Restored from checkpoint in {round(time_end - time_start, 4)} seconds")
        return self.manager, self.workers, self.files, self.tasks

