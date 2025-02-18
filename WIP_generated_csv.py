import argparse
import os
import copy
import json
import pandas as pd
from datetime import datetime
import re
from tqdm import tqdm
from collections import defaultdict
import json
from bitarray import bitarray # type: ignore
from tqdm import tqdm
import re
from datetime import datetime, timezone, timedelta
import pytz
import numpy as np
from worker_info import WorkerInfo
from task_info import TaskInfo
from file_info import FileInfo


class ManagerInfo:
    def __init__(self, runtime_template):
        self.ip = None
        self.port = None
        self.transfer_port = None

        # log files
        self.runtime_template = runtime_template
        self.debug = os.path.join(runtime_template, 'debug')
        self.transactions = os.path.join(runtime_template, 'transactions')
        self.taskgraph = os.path.join(runtime_template, 'taskgraph')
        self.daskvine_log = os.path.join(runtime_template, 'daskvine.log')

        # time info
        self.time_zone_offset_hours = None
        self.time_start = None
        self.time_end = None
        self.when_first_task_start_commit = None
        self.when_last_task_done = None
        self.tasks_submitted = None
        self.tasks_done = None

        # tasks
        self.tasks = {}        # key: (task_id, task_try_id), value: TaskInfo
        self.current_try_id = {}   # key: task_id, value: task_try_id

        # workers
        self.workers = {}      # key: (ip, port), value: WorkerInfo
        self.ip_transfer_port_to_worker = {}     # key: (ip, transfer_port), value: WorkerInfo

        # files
        self.files = {}      # key: filename, value: FileInfo

        self.set_time_zone()

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

        # timestamps only have 2 decimal places, operations might happen before the manager connection, which is a bug needs to be fixed
        if len(str(unix_timestamp).split('.')[1]) == 2:
            unix_timestamp += 0.01
        elif len(str(unix_timestamp).split('.')[1]) == 1:
            unix_timestamp += 0.1
        elif len(str(unix_timestamp).split('.')[1]) == 0:
            unix_timestamp += 1
        return unix_timestamp
    
    def ensure_worker_entry(self, worker_ip: str, worker_port: int):
        worker_entry = (worker_ip, worker_port)
        if worker_entry not in self.workers:
            self.workers[worker_entry] = WorkerInfo(worker_ip, worker_port, self)
        return self.workers[worker_entry]
    
    def ensure_file_info_entry(self, filename: str, size_mb: int):
        if filename not in self.files:
            self.files[filename] = FileInfo(filename, size_mb)
        return self.files[filename]

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
                    timestamp = float(timestamp) / 1e6
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
                        continue
                    if status == 'WAITING_RETRIEVAL':
                        continue
                    if status == 'RETRIEVED':
                        continue
                    if status == 'DONE':
                        continue

                if event_type == 'WORKER':
                    if not obj_id.startswith('worker'):
                        raise ValueError(f"worker {obj_id} is not a worker")
                
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

                if "info" in parts and "worker-id" in parts:
                    # this is the first time a worker is connected to the manager
                    info_idx = parts.index("info")
                    ip, port =  WorkerInfo.extract_ip_port_from_string(parts[info_idx - 1])
                    worker = WorkerInfo(ip, port, self)
                    worker.set_hash(parts[info_idx + 2])
                    worker.set_machine_name(parts[info_idx - 2])
                    worker.add_connection(timestamp)
                    self.add_worker(worker)
                    continue

                if "removed" in parts:
                    release_idx = parts.index("removed")
                    ip, port = WorkerInfo.extract_ip_port_from_string(parts[release_idx - 1])
                    worker = self.workers[(ip, port)]
                    worker.add_disconnection(timestamp)
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
                    putting_transfer_event = file.start_new_transfer('manager', (worker.ip, worker.port), timestamp, 'manager_put', file_type, file_cache_level)
                    continue
                if "received" in parts:
                    putting_transfer_event.set_time_stage_in(timestamp)
                    putting_transfer_event.set_state("worker_received")
                    putting_transfer_event = None
                    continue

                if "resources" in parts:
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

                if "puturl" in parts or "puturl_now" in parts:
                    puturl_id = parts.index("puturl") if "puturl" in parts else parts.index("puturl_now")
                    filename = parts[puturl_id + 2]
                    file_cache_level = parts[puturl_id + 3]
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
                    file.start_new_transfer((source_worker.ip, source_worker.port), (dest_worker.ip, dest_worker.port), timestamp, transfer_event, 2, file_cache_level)

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
                        task.add_input_file(parts[parts.index("infile") + 1])
                    elif "outfile" in parts:
                        task.add_output_file(parts[parts.index("outfile") + 1])
                    continue

                if "state change:" in line:
                    task_id = int(parts[parts.index("Task") + 1])
                    if "INITIAL (0) to READY (1)" in line:                  # a brand new task
                        assert task_id not in self.current_try_id
                        # new task entry
                        self.current_try_id[task_id] += 1
                        task = TaskInfo(task_id, self.current_try_id[task_id], self)
                        self.add_task(task)
                        continue

                    task_entry = (task_id, self.current_try_id[task_id])
                    task = self.tasks[task_entry]
                    if "READY (1) to RUNNING (2)" in line:                  # as expected 
                        task.when_running = timestamp
                        # update the coremap
                        worker_entry = (task.worker_ip, task.worker_port)
                        worker = self.workers[worker_entry]
                        worker.run_task(task)
                    elif "RUNNING (2) to WAITING_RETRIEVAL (3)" in line:    # as expected
                        task.when_waiting_retrieval = timestamp
                        # update the coremap
                        worker_entry = (task.worker_ip, task.worker_port)
                        worker = self.workers[worker_entry]
                        worker.reap_task(task)
                    elif "WAITING_RETRIEVAL (3) to RETRIEVED (4)" in line:  # as expected
                        task.when_retrieved = timestamp
                    elif "RETRIEVED (4) to DONE (5)" in line:               # as expected
                        task.when_done = timestamp
                    elif "WAITING_RETRIEVAL (3) to READY (1)" in line or \
                         "RUNNING (2) to READY (1)" in line:                # task failure
                        task.when_next_ready = timestamp
                        # new task entry
                        self.current_try_id[task_id] += 1
                        new_task = TaskInfo(task_id, self.current_try_id[task_id], self)
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
                    sandbox_used = int(parts[complete_idx + 7])
                    task_id = int(parts[complete_idx + 8])

                    task_entry = (task_id, self.current_try_id[task_id])
                    task = self.tasks[task_entry]
                    task.set_task_status(task_status)
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
                    # a temporary hack
                    if task_id < 0:
                        print(f"Warning: task id {task_id} is negative")
                        continue
                    if task_id not in self.current_try_id:
                        self.current_try_id[task_id] = 1    # this is the first try

                    task_entry = (task_id, self.current_try_id[task_id])
                    self.tasks[task_entry].when_input_transfer_ready = timestamp

                if "cache-update" in parts:
                    # cache-update cachename, &type, &cache_level, &size, &mtime, &transfer_time, &start_time, id
                    cache_update_id = parts.index("cache-update")
                    ip, port = WorkerInfo.extract_ip_port_from_string(parts[cache_update_id - 1])
                    worker = self.workers[(ip, port)]

                    filename = parts[cache_update_id + 1]
                    file_type = parts[cache_update_id + 2]
                    file_cache_level = parts[cache_update_id + 3]
                    size_in_mb = int(parts[cache_update_id + 4]) / 2**20

                    # if this is a task-generated file, it is the first time the file is cached on this worker, otherwise we only update the stage in time
                    file = self.ensure_file_info_entry(filename, size_in_mb)

                    # find all pending transfers on the destination
                    pending_transfers_on_destination = file.get_transfers_on_destination(worker.ip, 'pending')

                    # if there are pending transfers on the destination, the file was transferred by other sources
                    if pending_transfers_on_destination:
                        for transfer in pending_transfers_on_destination:
                            transfer.set_time_stage_in(timestamp)
                            transfer.set_state("cache_update")
                    # otherwise, the file is created by the current task
                    else:
                        # todo: the start stage in time should be the time when the task ends on the worker
                        transfer = file.start_new_transfer('Task', (worker.ip, worker.port), timestamp, 'task_created', file_type, file_cache_level)
                        transfer.set_time_stage_in(timestamp)
                        transfer.set_state("cache_update")

                    continue
                    
                if "cache-invalid" in parts:
                    cache_invalid_id = parts.index("cache-invalid")
                    ip, port = WorkerInfo.extract_ip_port_from_string(parts[cache_invalid_id - 1])
                    worker = self.workers[(ip, port)]
                    filename = parts[cache_invalid_id + 1]

                    file = self.files[filename]
                    file.cache_invalid_on_destination(timestamp, (ip, port))
                    continue

                if "unlink" in parts:
                    unlink_id = parts.index("unlink")
                    filename = parts[unlink_id + 1]
                    ip, port = WorkerInfo.extract_ip_port_from_string(parts[unlink_id - 1])
                    worker = self.workers[(ip, port)]

                    file = self.files[filename]
                    file.unlink_on_destination(timestamp, (ip, port))

                    """
                    # This part is handled in initialize_files()
                    failed_stage_in_count = len(file_transfer.when_start_stage_in) - len(file_transfer.when_stage_in)

                    # in some case when using puturl or puturl_now, we may fail to receive the cache-update message, use the start time as the stage in time
                    if failed_stage_in_count > 0:
                        # in this case, some files are put by the manager, either puturl or puturl_now, but eventually that file didn't go to the worker
                        # theoretically, there should be only 1 failed count
                        print(f"Warning: unlink file {filename} on {ip}:{port} failed to be transferred, failed_count: {failed_stage_in_count}")
                        for i in range(1, failed_stage_in_count + 1):
                            # get the source of the failed transfer
                            source = file_transfer.source[-i]
                            cache_level = file_transfer.cache_level[-i]
                            when_start_stage_in = file_transfer.when_start_stage_in[-i]
                            when_stage_out = timestamp
                            print(f"  - {i}: source: {source}, cache_level: {cache_level}, after {round(when_stage_out - when_start_stage_in, 4)} seconds")
                    else:
                        # todo: this indicates the fully lost file, update the producer tasks' when_output_fully_lost if any
                        pass
                    """ 
                    
                if "Submitted" in parts and "recovery" in parts and "task" in parts:
                    task_id = int(parts[parts.index("task") + 1])
                    task_try_id = self.current_try_id[task_id]
                    task = self.tasks[(task_id, task_try_id)]
                    task['is_recovery_task'] = True

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
                    getting_transfer_event = file.start_new_transfer((source_ip, source_port), 'manager', timestamp, 'manager_get', 1, 1)
                    continue
                if getting_transfer_event and "sent" in parts:
                    getting_transfer_event.set_time_stage_in(timestamp)
                    getting_transfer_event.set_state("manager_received")
                    getting_transfer_event = None
                    continue

                if "manager end" in line:
                    self.time_end = timestamp

            pbar.close()

    def generate_manager_disk_usage_csv(self):
        manager_disk_usage = {}
        pass

    def assign_worker_id_to_workers(self):
        # worker connected earlier has smaller worker_id, starting from 1
        worker_id = 1
        for worker in sorted(self.workers.values(), key=lambda x: x.time_connected[0]):
            worker.worker_id = worker_id
            worker_id += 1

    def initialize_files(self):
        for task in self.tasks.values():
            for filename in list(task.input_files):
                if filename not in self.files:
                    self.files[filename] = FileInfo(filename, 0)
                self.files[filename].consumers.add(task.task_id)
            for filename in list(task.output_files):
                if filename not in self.files:
                    self.files[filename] = FileInfo(filename, 0)
                self.files[filename].producers.add(task.task_id)
        
        for worker in self.workers.values():
            for filename, file_transfer in worker.file_transfers.items():
                if filename not in self.files:
                    raise ValueError(f"file {filename} not in files")
                file_info = self.files[filename]
                file_info.size_in_mb = file_transfer.size_mb

                holding_count = len(file_transfer.when_stage_in)
                for i in range(holding_count):
                    worker_id = worker.worker_id
                    time_when_stage_in = file_transfer.when_stage_in[i]
                    time_when_stage_out = file_transfer.when_stage_out[i]
                    holding_time = time_when_stage_out - time_when_stage_in
                    file_info.worker_holding.append([worker_id, time_when_stage_in, time_when_stage_out, holding_time])

                # some transfers may failed out of no reason, set the state to "pending"
                if len(file_transfer.state) < len(file_transfer.when_start_stage_in):
                    for i in range(len(file_transfer.when_start_stage_in) - len(file_transfer.state)):
                        file_transfer.state.append("pending")

                for i in range(len(file_transfer.state)):
                    state = file_transfer.state[i]
                    if state == "pending":
                        time_start_stage_in = file_transfer.when_start_stage_in[i]
                        source = f"{file_transfer.source[i][0]}:{file_transfer.source[i][1]}"
                        dest = f"{file_transfer.destination[i][0]}:{file_transfer.destination[i][1]}"
                        # find the worker disconnected time
                        matched_disconnected_time = None
                        for time_connected, time_disconnected in zip(worker.time_connected, worker.time_disconnected):
                            if time_connected <= time_start_stage_in <= time_disconnected:
                                matched_disconnected_time = time_disconnected
                                break
                        if matched_disconnected_time is None:
                            raise ValueError(f"no worker disconnected time found for file {file_transfer.filename}")
                        pending_time = round(matched_disconnected_time - time_start_stage_in, 4)
                        print(f"Warning: file {file_transfer.filename} received status is {state}, dest: {dest}, source: {source}, event: {file_transfer.event[i]}, after pending {pending_time} seconds")

                        # if the file was not staged out, set the stage out time to the worker disconnected time
                        try:
                            file_transfer.when_stage_out[i]
                        except IndexError:
                            file_transfer.append_when_stage_out(matched_disconnected_time)
                        continue
                    if state == "cache_invalid":
                        continue
                    elif state == "cache_update" or state == "worker_received":
                        worker_id = worker.worker_id
                        time_stage_in = file_transfer.when_stage_in[i]
                        time_stage_out = file_transfer.when_stage_out[i]
                        holding_time = time_stage_out - time_stage_in
                        file_info.worker_holding.append([worker_id, time_stage_in, time_stage_out, holding_time])
                        continue
                    else:
                        raise ValueError(f"pending received status: {state}")

"""
        # manager_disk_usage can be immediately transferred to manager_disk_usage_df
        manager_disk_usage_df = pd.DataFrame.from_dict(manager_disk_usage, orient='index')
        manager_disk_usage_df.index.name = 'filename'
        if 'size(MB)' not in manager_disk_usage_df.columns:
            manager_disk_usage_df['size(MB)'] = 0
        manager_disk_usage_df["accumulated_disk_usage(MB)"] = manager_disk_usage_df["size(MB)"].cumsum()
        manager_disk_usage_df.to_csv(os.path.join(dirname, 'manager_disk_usage.csv'))

        for worker_hash, worker in worker_info.items():
            for filename, worker_file_transfers in worker['file_transfers'].items():
                len_stage_in = len(worker_file_transfers['when_stage_in'])
                len_stage_out = len(worker_file_transfers['when_stage_out'])
                if len_stage_in < len_stage_out:
                    worker_file_transfers['when_stage_out'] = worker_file_transfers['when_stage_out'][:len_stage_in]
                    len_stage_out = len_stage_in
                if filename not in file_info:
                    raise ValueError(f"file {filename} not in file_info")
                # add the worker holding information
                for i in range(len_stage_out):
                    worker_holding = {
                        'worker_hash': worker_hash,
                        'time_stage_in': worker_file_transfers['when_stage_in'][i],
                        'time_stage_out': worker_file_transfers['when_stage_out'][i],
                    }
                    file_info[filename]['worker_holding'].append(worker_holding)

                # in case some files are not staged out, consider the manager end time as the stage out time
                if len_stage_out < len_stage_in:
                    print(f"Warning: file {filename} stage out less than stage in for worker {worker_hash}, stage_in: {len_stage_in}, stage_out: {len_stage_out}")
                    for i in range(len_stage_in - len_stage_out):
                        worker_holding = {
                            'worker_hash': worker_hash,
                            'time_stage_in': worker_file_transfers['when_stage_in'][len_stage_in - i - 1],
                            'time_stage_out': manager_info['time_end'],
                        }
                        file_info[filename]['worker_holding'].append(worker_holding)

        # filter out the workers that are not active
        manager_info['total_workers'] = len(worker_info)
        active_workers = set()
        for task in task_info.values():
            active_workers.add(task['committed_worker_hash'])
        worker_info = {worker_hash: worker for worker_hash, worker in worker_info.items() if worker_hash in active_workers}
        worker_info = {k: v for k, v in sorted(worker_info.items(), key=lambda item: item[1]['time_connected'])}
        manager_info['active_workers'] = len(worker_info)

        # Add worker_id to worker_info and update the relevant segments in task_info and library_info
        worker_id = 1
        for worker in worker_info.values():
            worker['worker_id'] = worker_id
            worker_id += 1
        for task in task_info.values():
            if task['committed_worker_hash']:
                task['worker_id'] = worker_info[task['committed_worker_hash']]['worker_id']
        for library in library_info.values():
            if library['committed_worker_hash']:
                library['worker_id'] = worker_info[library['committed_worker_hash']]['worker_id']
        
        with open(os.path.join(dirname, 'worker_info.json'), 'w') as f:
            json.dump(worker_info, f, indent=4)
"""

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('runtime_template', type=str, help='list of log directories')
    parser.add_argument('--execution-details-only', action='store_true', help='Only generate data for task execution details')
    parser.add_argument('--meta-files', action='store_true', help='include meta files in the file_info.csv')
    args = parser.parse_args()

    runtime_template = os.path.join(args.runtime_template, 'vine-logs')

    manager_info = ManagerInfo(runtime_template)

    manager_info.parse_debug()
    manager_info.parse_transactions()
    manager_info.assign_worker_id_to_workers()
    manager_info.initialize_files()