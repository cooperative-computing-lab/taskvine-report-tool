import argparse
import os
import copy
import json
import pandas as pd
from datetime import datetime
import re
from tqdm import tqdm
import json
from bitarray import bitarray # type: ignore
from tqdm import tqdm
import re
from datetime import datetime, timezone, timedelta
import pytz
import numpy as np


# initialize the global variables
task_info, task_try_count, library_info, worker_info, manager_info, file_info, category_info = {}, {}, {}, {}, {}, {}, {}
worker_address_hash_map = {}
task_start_timestamp = 'time_worker_start'
task_finish_timestamp = 'time_worker_end'

############################################################################################################
# Helper functions
def datestring_to_timestamp(datestring):
    if manager_info['time_zone_offset_hours'] is None:
        print("Warning: time_zone_offset_hours is not set")
        exit(1)
    tz_custom = timezone(timedelta(hours=manager_info['time_zone_offset_hours']))
    datestring_custom = datetime.strptime(datestring, "%Y/%m/%d %H:%M:%S.%f").replace(tzinfo=tz_custom)
    unix_timestamp = float(datestring_custom.timestamp())
    return unix_timestamp

def timestamp_to_datestring(unix_timestamp):
    if manager_info['time_zone_offset_hours'] is None:
        print("Warning: time_zone_offset_hours is not set")
        exit(1)
    tz_custom = timezone(timedelta(hours=manager_info['time_zone_offset_hours']))
    datestring_custom = datetime.fromtimestamp(unix_timestamp, tz=tz_custom).strftime("%Y/%m/%d %H:%M:%S.%f")
    return datestring_custom

def set_time_zone(datestring):
    mgr_start_datesting = datetime.strptime(datestring, "%Y/%m/%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")
    formatted_timestamp = int(manager_info['time_start'])
    utc_datestring = datetime.fromtimestamp(formatted_timestamp, timezone.utc)
    for tz in pytz.all_timezones:
        tz_datestring = utc_datestring.replace(tzinfo=pytz.utc).astimezone(pytz.timezone(tz)).strftime('%Y-%m-%d %H:%M:%S')
        if mgr_start_datesting == tz_datestring:
            manager_info['time_zone_offset_hours'] = int(pytz.timezone(tz).utcoffset(datetime.now()).total_seconds() / 3600)
            
def get_worker_ip_port_by_hash(worker_address_hash_map, worker_hash):
    # worker_address_hash_map: {(ip, port): hash}
    workers_by_ip_port = []
    for k, v in worker_address_hash_map.items():
        if v == worker_hash:
            workers_by_ip_port.append(k[0] + ":" + k[1])
    return workers_by_ip_port

def get_worker_hash(worker_ip_port_string):
    content = re.search(r'\((.*?)\)', worker_ip_port_string).group(1)
    worker_ip, worker_port = content.split(':')
    return worker_address_hash_map[(worker_ip, worker_port)]

def update_file_size(filename, size_in_mb):
    if filename not in file_info:
        raise ValueError(f"file {filename} not in file_info")
    if file_info[filename]['size(MB)'] == 0:
        file_info[filename]['size(MB)'] = size_in_mb
    else:
        if file_info[filename]['size(MB)'] != size_in_mb:
            raise ValueError(f"file {filename} size mismatch: {file_info[filename]['size(MB)']} vs {size_in_mb}")

############################################################################################################

############################################################################################################
# Parse functions
def parse_txn():
    worker_coremap = {}

    total_lines = 0
    with open(txn, 'r') as file:
        for line in file:
            total_lines += 1

    with open(txn, 'r') as file:
        pbar = tqdm(total=total_lines, desc="parsing transactions")
        for line in file:
            pbar.update(1)

            if line.startswith("#"):
                continue

            timestamp, _, event_type, obj_id, status, *info = line.split(maxsplit=5)

            try:
                timestamp = float(timestamp) / 1e6
            except ValueError:
                continue

            info = info[0] if info else "{}"

            if event_type == 'TASK':
                task_id = int(obj_id)
                if status == 'READY':
                    if task_id not in task_try_count:
                        task_try_count[task_id] = 1
                    else:
                        task = task_info[(task_id, task_try_count[task_id])]
                        task['when_next_ready'] = timestamp
                        # reset the coremap for the new try
                        for i in task['core_id']:
                            worker_coremap[task['worker_committed']][i] = 0
                        worker_info[task['worker_committed']]['tasks_failed'].append(task_id)
                        task_try_count[task_id] += 1
                    task_category = info.split()[0]
                    try_id = task_try_count[task_id]
                    resources_requested = json.loads(info.split(' ', 3)[-1])
                    task = {
                        'task_id': task_id,
                        'try_id': try_id,
                        'worker_id': -1,
                        'core_id': [],
                        'execution_time': None,           # spans from time_worker_start to time_worker_end

                        # Timestamps throughout the task lifecycle
                        'when_ready': timestamp,          # ready status on the manager
                        'time_commit_start': None,              # start commiting to worker
                        'time_commit_end': None,                # end commiting to worker
                        'when_running': None,             # running status on worker
                        'time_worker_start': None,              # start executing on worker
                        'time_worker_end': None,                # end executing on worker
                        'when_waiting_retrieval': None,   # waiting for retrieval status on worker
                        'when_retrieved': None,           # retrieved status on worker
                        'when_done': None,                # done status on worker
                        'when_next_ready': None,          # only for on-worker failed tasks

                        'when_output_fully_lost': None,

                        'worker_committed': None,

                        'size_input_mgr': None,
                        'size_output_mgr': None,
                        'cores_requested': resources_requested.get("cores", [0, ""])[0],
                        'gpus_requested': resources_requested.get("gpus", [0, ""])[0],
                        'memory_requested(MB)': resources_requested.get("memory", [0, ""])[0],
                        'disk_requested(MB)': resources_requested.get("disk", [0, ""])[0],
                        'retrieved_status': None,
                        'done_status': None,
                        'done_code': None,
                        'category': task_category,
                        'category_id': None,

                        'input_files': [],
                        'output_files': [],
                        'size_input_files(MB)': 0,
                        'size_output_files(MB)': 0,
                        'critical_parent': None,                # task_id of the most recent ready parent
                        'critical_input_file': None,            # input file that took the shortest time to use
                        'critical_input_file_wait_time': 0,     # wait time from when the input file was ready to when it was used
                        'is_recovery_task': False,

                        'graph_id': -1,                         # will be set in dag part
                        'schedule_id': -1,                      # order of scheduling

                    }
                    if task['cores_requested'] == 0:
                        task['cores_requested'] = 1
                    task_info[(task_id, try_id)] = task
                if status == 'RUNNING':
                    # a running task can be a library which does not have a ready status
                    resources_allocated = json.loads(info.split(' ', 3)[-1])
                    if task_id in task_try_count:
                        try_id = task_try_count[task_id]
                        task = task_info[(task_id, try_id)]
                        worker_hash = info.split()[0]
                        task['when_running'] = timestamp
                        task['worker_committed'] = worker_hash
                        task['time_commit_start'] = float(resources_allocated["time_commit_start"][0])
                        task['time_commit_end'] = float(resources_allocated["time_commit_end"][0])
                        task['size_input_mgr'] = float(resources_allocated["size_input_mgr"][0])
                        coremap = worker_coremap[worker_hash]
                        cores_found = 0
                        for i in range(1, len(coremap)):
                            if coremap[i] == 0:
                                coremap[i] = 1
                                task['core_id'].append(i)
                                cores_found += 1
                                if cores_found == task['cores_requested']:
                                    break
                    else:
                        library = {
                            'task_id': task_id,
                            'when_running': timestamp,
                            'time_commit_start': resources_allocated["time_commit_start"][0],
                            'time_commit_end': resources_allocated["time_commit_end"][0],
                            'when_sent': None,
                            'when_started': None,
                            'when_retrieved': None,
                            'worker_committed': info.split(' ', 3)[0],
                            'worker_id': -1,
                            'size_input_mgr': resources_allocated["size_input_mgr"][0],
                            'cores_requested': resources_allocated.get("cores", [0, ""])[0],
                            'gpus_requested': resources_allocated.get("gpus", [0, ""])[0],
                            'memory_requested(MB)': resources_allocated.get("memory", [0, ""])[0],
                            'disk_requested(MB)': resources_allocated.get("disk", [0, ""])[0],
                        }
                        library_info[task_id] = library
                if status == 'WAITING_RETRIEVAL':
                    if task_id in task_try_count:
                        task = task_info[(task_id, task_try_count[task_id])]
                        task['when_waiting_retrieval'] = timestamp
                        worker_hash = task['worker_committed']
                        for core in task['core_id']:
                            worker_coremap[worker_hash][core] = 0
                if status == 'RETRIEVED':
                    try:
                        resources_retrieved = json.loads(info.split(' ', 5)[-1])
                    except json.JSONDecodeError:
                        resources_retrieved = {}
                    if task_id in task_try_count:
                        task = task_info[(task_id, task_try_count[task_id])]
                        task['when_retrieved'] = timestamp
                        task['retrieved_status'] = status
                        task['time_worker_start'] = resources_retrieved.get("time_worker_start", [None])[0]
                        task['time_worker_end'] = resources_retrieved.get("time_worker_end", [None])[0]
                        task['execution_time'] = task['time_worker_end'] - task['time_worker_start']
                        task['size_output_mgr'] = resources_retrieved.get("size_output_mgr", [None])[0]
                    else:
                        library = library_info[task_id]
                        library['when_retrieved'] = timestamp
                if status == 'DONE':
                    done_info = info.split() if info else []
                    if task_id in task_try_count:
                        task = task_info[(task_id, task_try_count[task_id])]
                        worker_hash = task['worker_committed']
                        task['when_done'] = timestamp
                        task['done_status'] = done_info[0] if len(done_info) > 0 else None
                        task['done_code'] = done_info[1] if len(done_info) > 1 else None
                        if task_id in worker_info[worker_hash]['tasks_completed']:
                            print(f"Warning: task {task_id} is completed twice on worker {worker_hash}")
                        worker_info[worker_hash]['tasks_completed'].append(task_id)
                        # update category_info
                        task_category = task['category']
                        execution_time = round(task[task_finish_timestamp] - task[task_start_timestamp], 4)
                        if task_category not in category_info:
                            category_info[task_category] = {
                                'category_id': int(len(category_info) + 1),  # starts from 1
                                'tasks': [],
                                'tasks_execution_time(s)': [],
                            }
                        category_info[task_category]['tasks'].append(task_id)
                        category_info[task_category]['tasks_execution_time(s)'].append(execution_time)
                        task['category_id'] = category_info[task_category]['category_id']
            if event_type == 'WORKER':
                if not obj_id.startswith('worker'):
                    continue
                if status == 'CONNECTION':
                    if obj_id not in worker_info:
                        worker_info[obj_id] = {
                            'time_connected': [timestamp],
                            'time_disconnected': [],
                            'worker_id': -1,
                            'worker_machine_name': None,
                            'worker_ip': None,
                            'worker_port': None,
                            'tasks_completed': [],
                            'tasks_failed': [],
                            'num_tasks_completed': 0,
                            'num_tasks_failed': 0,
                            'cores': None,
                            'memory(MB)': None,
                            'disk(MB)': None,
                            'disk_update': {},
                        }
                    else:
                        worker_info[obj_id]['time_connected'].append(timestamp)
                elif status == 'DISCONNECTION':
                    worker_info[obj_id]['time_disconnected'].append(timestamp)
                elif status == 'RESOURCES':
                    # only parse the first resources reported
                    if worker_info[obj_id]['cores'] is not None:
                        continue
                    resources = json.loads(info)
                    cores, memory, disk = resources.get("cores", [0, ""])[0], resources.get("memory", [0, ""])[0], resources.get("disk", [0, ""])[0]
                    worker_info[obj_id]['cores'] = cores
                    worker_info[obj_id]['memory(MB)'] = memory
                    worker_info[obj_id]['disk(MB)'] = disk
                    # for calculating task core_id
                    worker_coremap[obj_id] = bitarray(cores + 1)
                    worker_coremap[obj_id].setall(0)
                elif status == 'TRANSFER' or status == 'CACHE_UPDATE':
                    if status == 'TRANSFER':
                        # don't consider transfer as of now
                        pass
                    elif status == 'CACHE_UPDATE':
                        # will handle in debug parsing
                        pass

            if event_type == 'LIBRARY':
                if status == 'SENT':
                    for library in library_info.values():
                        if library['task_id'] == obj_id:
                            library['when_sent'] = timestamp
                if status == 'STARTED':
                    for library in library_info.values():
                        if library['task_id'] == obj_id:
                            library['when_started'] = timestamp
            if event_type == 'MANAGER':
                if status == 'START':
                    manager_info['time_start'] = timestamp
                    manager_info['time_end'] = None
                    manager_info['lifetime(s)'] = None
                    manager_info['time_start_human'] = None
                    manager_info['time_end_human'] = None
                    manager_info['tasks_submitted'] = 0
                    manager_info['tasks_done'] = 0
                    manager_info['tasks_failed_on_manager'] = 0
                    manager_info['tasks_failed_on_worker'] = 0
                    manager_info['max_task_try_count'] = 0
                    manager_info['total_workers'] = 0
                    manager_info['max_concurrent_workers'] = 0
                    manager_info['failed'] = 0
                    manager_info['time_zone_offset_hours'] = None

                if status == 'END':
                    manager_info['time_end'] = timestamp
                    manager_info['lifetime(s)'] = round(manager_info['time_end'] - manager_info['time_start'], 2)
        pbar.close()

    if manager_info['time_end'] is None:
        # if the manager did not end, set the end time to the last txn timestamp
        manager_info['time_end'] = timestamp
        manager_info['lifetime(s)'] = round(manager_info['time_end'] - manager_info['time_start'], 2)
        manager_info['failed'] = True


def parse_taskgraph():
    total_lines = 0
    with open(taskgraph, 'r') as file:
        for line in file:
            total_lines += 1

    with open(taskgraph, 'r') as file:
        pbar = tqdm(total=total_lines, desc="parsing taskgraph")
        line_id = 0
        for line in file:
            line_id += 1
            pbar.update(1)
            if '->' not in line:
                if line.startswith('"'):
                    left, right = line.split(' ')
                    filename = left[1:-1]
                    if filename.startswith('file'):
                        filename = filename[5:]
                        if filename not in file_info:
                            file_info[filename] = {
                                'size(MB)': 0,
                                'producers': [],
                                'consumers': [],
                                'worker_holding': [],
                            }
            else:
                try:
                    left, right = line.split(' -> ')
                    left = left.strip().strip('"')
                    right = right.strip()[:-1].strip('"')
                except ValueError:
                    print(f"Warning: Unexpected format: {line}")
                    continue

                try:
                    # task -> file
                    if left.startswith('task'):
                        filename = right.split('-', 1)[1]
                        task_id = int(left.split('-')[1])
                        try_id = task_try_count[task_id]
                        task_info[(task_id, try_id)]['output_files'].append(filename)
                        if filename not in file_info:
                            file_info[filename] = {
                                'size(MB)': 0,
                                'producers': [],
                                'consumers': [],
                                'worker_holding': [],
                            }
                        file_info[filename]['producers'].append(task_id)
                    # file -> task
                    elif right.startswith('task'):
                        filename = left.split('-', 1)[1]
                        task_id = int(right.split('-')[1])
                        try_id = task_try_count[task_id]
                        task_info[(task_id, try_id)]['input_files'].append(filename)
                        if filename not in file_info:
                            file_info[filename] = {
                                'size(MB)': 0,
                                'producers': [],
                                'consumers': [],
                                'worker_holding': [],
                            }
                        file_info[filename]['consumers'].append(task_id)
                except IndexError:
                        print(f"Warning: Unexpected format: {line}")
                        continue
        pbar.close()

        # we only consider files produced by another task as input files
        for task in task_info.values():
            cleaned_input_files = []
            for input_file in task['input_files']:
                if file_info[input_file]['producers']:
                    cleaned_input_files.append(input_file)
            task['input_files'] = cleaned_input_files


def parse_debug():
    global worker_info
    total_lines = 0
    with open(debug, 'r') as file:
        for line in file:
            total_lines += 1

    # put a file on a worker
    putting_file = False
    putting_filename = None

    # send task info to a worker
    sending_task_id = None
    sending_task_try_id = {}
    for task_info_key in task_info.keys():
        task_id, try_id = task_info_key
        sending_task_try_id[task_id] = 1

    with open(debug, 'r') as file:
        pbar = tqdm(total=total_lines, desc="parsing debug")
        for line in file:
            pbar.update(1)
            parts = line.strip().split(" ")

            if "manager" in parts and "start" in parts:
                datestring = parts[0] + " " + parts[1]
                set_time_zone(datestring)

            if "info" in parts and "worker-id" in parts:
                worker_id_id = parts.index("worker-id")
                worker_hash = parts[worker_id_id + 1]
                worker_machine_name = parts[worker_id_id - 3]
                worker_ip, worker_port = parts[worker_id_id - 2][1:-2].split(':')
                worker_address_hash_map[(worker_ip, worker_port)] = worker_hash
                if worker_hash in worker_info:
                    worker_info[worker_hash]['worker_machine_name'] = worker_machine_name
                    worker_info[worker_hash]['worker_ip'] = worker_ip
                    worker_info[worker_hash]['worker_port'] = worker_port

            if "put" in parts:
                putting_file = True
                continue
            if putting_file:
                if "file" in parts and parts[parts.index("file") - 1].endswith(':'):
                    file_id = parts.index("file")
                    worker_hash = get_worker_hash(parts[file_id - 1])
                    putting_filename = parts[file_id + 1]
                    size_in_mb = int(parts[file_id + 2]) / 2**20

                    datestring = parts[0] + " " + parts[1]
                    timestamp = datestring_to_timestamp(datestring)
                    if (timestamp > manager_info['time_end']):
                        print(f"Warning: put start time {timestamp} of file {putting_filename} is after manager end time {manager_info['time_end']}, probably a time zone issue")
                    if timestamp < manager_info['time_start']:
                        if abs(timestamp - manager_info['time_start']) < 1:
                            # manager_info['time_start'] is more accurate
                            timestamp = worker_info[worker_hash]['time_connected'][0]
                        elif timestamp == 0:
                            # we have a special file with start time 0
                            timestamp = worker_info[worker_hash]['time_connected'][0]
                        else:
                            print(f"Warning: put start time {timestamp} of file {putting_filename} on worker {worker_hash} is before manager start time {manager_info['time_start']}")
                    # this is the first time the file is cached on this worker
                    # assume the start time is the same as the stage in time if put by the manager
                    if putting_filename not in worker_info[worker_hash]['disk_update']:
                        worker_info[worker_hash]['disk_update'][putting_filename] = {
                            'size(MB)': size_in_mb,
                            'when_start_stage_in': [timestamp],
                            'when_stage_in': [],
                            'when_stage_out': [],
                        }
                        update_file_size(putting_filename, size_in_mb)
                    else:
                        worker_info[worker_hash]['disk_update'][putting_filename]['when_start_stage_in'].append(timestamp)
                elif "received" in parts:
                    if putting_filename is None:
                        raise ValueError("putting_filename is None")
                    received_id = parts.index("received")
                    worker_hash = get_worker_hash(parts[received_id - 1])
                    datestring = parts[0] + " " + parts[1]
                    timestamp = datestring_to_timestamp(datestring)
                    if putting_filename not in worker_info[worker_hash]['disk_update']:
                        raise ValueError(f"file {putting_filename} not in worker {worker_hash}")
                    worker_info[worker_hash]['disk_update'][putting_filename]['when_stage_in'].append(timestamp)
                    putting_file = False
                    putting_filename = None

            if "puturl" in parts or "puturl_now" in parts:
                puturl_id = parts.index("puturl") if "puturl" in parts else parts.index("puturl_now")
                url_source = parts[puturl_id + 1]
                worker_hash = get_worker_hash(parts[puturl_id - 1])
                filename = parts[puturl_id + 2]
                cache_level = parts[puturl_id + 3]
                size_in_mb = int(parts[puturl_id + 4]) / 2**20
                datestring = parts[0] + " " + parts[1]
                timestamp = datestring_to_timestamp(datestring)

                # update disk usage
                if filename not in worker_info[worker_hash]['disk_update']:
                    # this is the first time the file is cached on this worker
                    worker_info[worker_hash]['disk_update'][filename] = {
                        'size(MB)': size_in_mb,
                        'when_start_stage_in': [timestamp],
                        'when_stage_in': [],
                        'when_stage_out': [],
                    }
                    update_file_size(filename, size_in_mb)
                else:
                    # already cached previously, start a new cache here
                    worker_info[worker_hash]['disk_update'][filename]['when_start_stage_in'].append(timestamp)

            if "cache-update" in parts:
                # cache-update cachename, &type, &cache_level, &size, &mtime, &transfer_time, &start_time, id
                # type: VINE_FILE=1, VINE_URL=2, VINE_TEMP=3, VINE_BUFFER=4, VINE_MINI_TASK=5
                # cache_level: 
                #    VINE_CACHE_LEVEL_TASK = 0,     /**< Do not cache file at worker. (default) */
                #    VINE_CACHE_LEVEL_WORKFLOW = 1, /**< File remains in cache of worker until workflow ends. */
                #    VINE_CACHE_LEVEL_WORKER = 2,   /**< File remains in cache of worker until worker terminates. */
                #    VINE_CACHE_LEVEL_FOREVER = 3   /**< File remains at execution site when worker terminates. (use with caution) */

                cache_update_id = parts.index("cache-update")
                filename = parts[cache_update_id + 1]
                file_type = parts[cache_update_id + 2]
                cache_level = parts[cache_update_id + 3]

                size_in_mb = int(parts[cache_update_id + 4]) / 2**20
                wall_time = float(parts[cache_update_id + 6]) / 1e6
                start_time = float(parts[cache_update_id + 7]) / 1e6

                worker_hash = get_worker_hash(parts[cache_update_id - 1])

                # start time should be after the manager start time
                if start_time < manager_info['time_start']:
                    # consider xxx.04224 and xxx.0 as the same time
                    if abs(start_time - manager_info['time_start']) < 1:
                        start_time = manager_info['time_start']
                    else:
                        print(f"Warning: cache-update start time {start_time} is before manager start time {manager_info['time_start']}")

                # update disk usage
                if filename not in worker_info[worker_hash]['disk_update']:
                    # this is the first time the file is cached on this worker
                    worker_info[worker_hash]['disk_update'][filename] = {
                        'size(MB)': size_in_mb,
                        'when_start_stage_in': [start_time],
                        'when_stage_in': [start_time + wall_time],
                        'when_stage_out': [],
                    }
                    update_file_size(filename, size_in_mb)
                else:
                    # the start time has been indicated in the puturl message, so we don't need to update it here
                    worker_info[worker_hash]['disk_update'][filename]['when_stage_in'].append(start_time + wall_time)

            if "task" in parts and "tx" in parts and "to" in parts and parts.index("task") == len(parts) - 2:
                sending_task_id = int(parts[parts.index("task") + 1])
                continue
            if sending_task_id:
                task_try_id = sending_task_try_id[sending_task_id]
                if "end" in parts:
                    sending_task_try_id[sending_task_id] += 1
                    sending_task_id = None
                elif "cores" in parts:
                    cores_requested = int(float(parts[parts.index("cores") + 1]))
                    task_info[(sending_task_id, task_try_id)]['cores_requested'] = cores_requested
                elif "gpus" in parts:
                    gpus_requested = int(float(parts[parts.index("gpus") + 1]))
                    task_info[(sending_task_id, task_try_id)]['gpus_requested'] = gpus_requested
                elif "memory" in parts:
                    memory_requested = int(float(parts[parts.index("memory") + 1]))
                    task_info[(sending_task_id, task_try_id)]['memory_requested(MB)'] = memory_requested
                elif "disk" in parts:
                    disk_requested = int(float(parts[parts.index("disk") + 1]))
                    task_info[(sending_task_id, task_try_id)]['disk_requested(MB)'] = disk_requested
                continue

            if ("infile" in parts or "outfile" in parts) and "needs" not in parts:
                file_id = parts.index("infile") if "infile" in parts else parts.index("outfile")
                worker_hash = get_worker_hash(parts[file_id - 1])
                manager_site_name = parts[file_id + 2]

                # update disk usage
                if manager_site_name in worker_info[worker_hash]['disk_update']:
                    del worker_info[worker_hash]['disk_update'][manager_site_name]
            
            if "unlink" in parts:
                unlink_id = parts.index("unlink")
                filename = parts[unlink_id + 1]
                worker_ip, worker_port = parts[unlink_id - 1][1:-2].split(':')
                datestring = parts[0] + " " + parts[1]
                timestamp = datestring_to_timestamp(datestring)
                worker_hash = worker_address_hash_map[(worker_ip, worker_port)]
                worker_id = worker_info[worker_hash]['worker_id']

                if filename not in worker_info[worker_hash]['disk_update']:
                    print(f"Warning: file {filename} not in worker {worker_hash}")
                    print(f"workers: {get_worker_ip_port_by_hash(worker_address_hash_map, worker_hash)}")
                worker_when_start_stage_in = worker_info[worker_hash]['disk_update'][filename]['when_start_stage_in']
                worker_when_stage_in = worker_info[worker_hash]['disk_update'][filename]['when_stage_in']
                worker_when_stage_out = worker_info[worker_hash]['disk_update'][filename]['when_stage_out']
                worker_when_stage_out.append(timestamp)

                # in some case when using puturl or puturl_now, we may fail to receive the cache-update message, use the start time as the stage in time
                if len(worker_when_start_stage_in) != len(worker_when_stage_in):
                    for i in range(len(worker_when_start_stage_in) - len(worker_when_stage_in)):
                        worker_when_stage_in.append(worker_when_start_stage_in[len(worker_when_start_stage_in) - i - 1])

                # this indicates the fully lost file, update the producer's when_output_fully_lost if any
                if len(worker_when_stage_out) == len(worker_when_stage_in) and len(file_info[filename]['producers']) != 0:
                    producers = file_info[filename]['producers']
                    i = len(producers) - 1
                    while i >= 0:
                        producer = producers[i]
                        if task_info[(producer, task_try_count[producer])]['time_worker_end'] < timestamp:
                            task_info[(producer, task_try_count[producer])]['when_output_fully_lost'] = timestamp
                            break
                        i -= 1
                
            if "Submitted" in parts and "recovery" in parts and "task" in parts:
                task_id = int(parts[parts.index("task") + 1])
                try_count = task_try_count[task_id]
                for try_id in range(1, try_count + 1):
                    task_info[(task_id, try_id)]['is_recovery_task'] = True
                    task_info[(task_id, try_id)]['category'] = "recovery_task"
        pbar.close()

    for worker_hash, worker in worker_info.items():
        for filename, worker_disk_update in worker['disk_update'].items():
            len_stage_in = len(worker_disk_update['when_stage_in'])
            len_stage_out = len(worker_disk_update['when_stage_out'])
            if len_stage_in < len_stage_out:
                print(f"Warning: file {filename} stage out more than stage in for worker {worker_hash}, stage_in: {len_stage_in}, stage_out: {len_stage_out}")
                worker_disk_update['when_stage_out'] = worker_disk_update['when_stage_out'][:len_stage_in]
                len_stage_out = len_stage_in
            if filename not in file_info:
                raise ValueError(f"file {filename} not in file_info")
            # add the worker holding information
            for i in range(len_stage_out):
                worker_holding = {
                    'worker_hash': worker_hash,
                    'time_stage_in': worker_disk_update['when_stage_in'][i],
                    'time_stage_out': worker_disk_update['when_stage_out'][i],
                }
                file_info[filename]['worker_holding'].append(worker_holding)
            # in case some files are not staged out, consider the manager end time as the stage out time
            if len_stage_out < len_stage_in:
                print(f"Warning: file {filename} stage out less than stage in for worker {worker_hash}, stage_in: {len_stage_in}, stage_out: {len_stage_out}")
                for i in range(len_stage_in - len_stage_out):
                    worker_holding = {
                        'worker_hash': worker_hash,
                        'time_stage_in': worker_disk_update['when_stage_in'][len_stage_in - i - 1],
                        'time_stage_out': manager_info['time_end'],
                    }
                    file_info[filename]['worker_holding'].append(worker_holding)

    # filter out the workers that are not active
    manager_info['total_workers'] = len(worker_info)
    active_workers = set()
    for task in task_info.values():
        active_workers.add(task['worker_committed'])
    worker_info = {worker_hash: worker for worker_hash, worker in worker_info.items() if worker_hash in active_workers}
    worker_info = {k: v for k, v in sorted(worker_info.items(), key=lambda item: item[1]['time_connected'])}
    manager_info['active_workers'] = len(worker_info)

    # Add worker_id to worker_info and update the relevant segments in task_info and library_info
    worker_id = 1
    for worker in worker_info.values():
        worker['worker_id'] = worker_id
        worker_id += 1
    for task in task_info.values():
        if task['worker_committed']:
            task['worker_id'] = worker_info[task['worker_committed']]['worker_id']
    for library in library_info.values():
        if library['worker_committed']:
            library['worker_id'] = worker_info[library['worker_committed']]['worker_id']
    
    with open(os.path.join(dirname, 'worker_info.json'), 'w') as f:
        json.dump(worker_info, f, indent=4)

    store_file_info()


def store_file_info():
    # calculate the size of input and output files
    print(f"Generating file_info.csv...")
    for filename, info in file_info.items():
        active_worker_holding = []
        for record in info['worker_holding']:
            # an inactive worker, skip
            if record['worker_hash'] not in worker_info:
                continue
            worker_id = worker_info[record['worker_hash']]['worker_id']
            time_stage_in = round(record['time_stage_in'], 2)
            time_stage_out = round(record['time_stage_out'], 2)
            life_time = round(time_stage_out - time_stage_in, 2)
            active_worker_holding.append([worker_id, time_stage_in, time_stage_out, life_time])
        active_worker_holding.sort(key=lambda x: x[1])
        info['num_workers_holding'] = len(info['worker_holding'])
        del info['worker_holding']
        info['worker_holding'] = active_worker_holding
        # remove files that are not produced by any task
        if not info['producers']:
            continue

    # save the file_info into a csv file, should use filename as key
    file_info_df = pd.DataFrame.from_dict(file_info, orient='index')
    file_info_df.index.name = 'filename'
    file_info_df.to_csv(os.path.join(dirname, 'file_info.csv'))


def parse_daskvine_log():
    # check if the daskvine exists
    try:
        with open(daskvine_log, 'r') as file:
            pass
    except FileNotFoundError:
        return
    
    total_lines = 0
    with open(daskvine_log, 'r') as file:
        for line in file:
            total_lines += 1

    with open(daskvine_log, 'r') as file:
        pbar = tqdm(total=total_lines, desc="parsing daskvine log")
        for line in file:
            pbar.update(1)
            parts = line.strip().split(" ")

            event, timestamp, task_id = parts[0], int(parts[1]), int(parts[2])
            try_count = task_try_count[task_id]
            if event == "submitted":
                for try_id in range(1, try_count + 1):
                    task_info[(task_id, try_id)]['when_submitted_by_daskvine'] = timestamp
            if event == 'received':
                for try_id in range(1, try_count + 1):
                    task_info[(task_id, try_id)]['when_received_by_daskvine'] = timestamp

        pbar.close()
############################################################################################################


def generate_worker_summary(worker_disk_usage_df):
    print(f"Generating worker_summary.csv...")

    rows = []
    for worker_hash, info in worker_info.items():
        row = {
            'worker_id': info['worker_id'],
            'worker_hash': worker_hash,
            'worker_machine_name': info['worker_machine_name'],
            'worker_ip': info['worker_ip'],
            'worker_port': info['worker_port'],
            'time_connected': info['time_connected'],
            'time_disconnected': info['time_disconnected'],
            'lifetime(s)': 0,
            'cores': info['cores'],
            'memory(MB)': info['memory(MB)'],
            'disk(MB)': info['disk(MB)'],
            'tasks_completed': info['tasks_completed'],
            'tasks_failed': info['tasks_failed'],
            'num_tasks_completed': 0,
            'num_tasks_failed': 0,
            'avg_task_runtime(s)': 0,
            'peak_disk_usage(MB)': 0,
            'peak_disk_usage(%)': 0,
        }
        # calculate the number of tasks done by this worker
        row['num_tasks_completed'] = len(worker_info[worker_hash]['tasks_completed'])
        row['num_tasks_failed'] = len(worker_info[worker_hash]['tasks_failed'])
        # check if this worker has any disk updates
        if not worker_disk_usage_df.empty and worker_disk_usage_df['worker_hash'].isin([worker_hash]).any():
            row['peak_disk_usage(MB)'] = worker_disk_usage_df[worker_disk_usage_df['worker_hash'] == worker_hash]['disk_usage(MB)'].max()
            row['peak_disk_usage(%)'] = worker_disk_usage_df[worker_disk_usage_df['worker_hash'] == worker_hash]['disk_usage(%)'].max()
        # the worker may not complete any tasks
        if row['num_tasks_completed'] > 0:
            total_execution_time = 0
            for task_id in worker_info[worker_hash]['tasks_completed']:
                total_execution_time += task_info[(task_id, task_try_count[task_id])][task_finish_timestamp] - task_info[(task_id, task_try_count[task_id])][task_start_timestamp]
            row['avg_task_runtime(s)'] = total_execution_time / row['num_tasks_completed']
        if len(info['time_connected']) != len(info['time_disconnected']):
            info['time_disconnected'].append(manager_info['time_end'])
            # raise ValueError("time_connected and time_disconnected have different lengths.")
        for i in range(len(info['time_connected'])):
            row_copy = copy.deepcopy(row)
            row_copy['time_connected'] = info['time_connected'][i]
            row_copy['time_disconnected'] = info['time_disconnected'][i]
            row_copy['lifetime(s)'] = info['time_disconnected'][i] - info['time_connected'][i]
            rows.append(row_copy)

    worker_summary_df = pd.DataFrame(rows)
    worker_summary_df = worker_summary_df.sort_values(by=['worker_id'], ascending=[True])
    worker_summary_df.to_csv(os.path.join(dirname, 'worker_summary.csv'), index=False)

    return worker_summary_df

def generate_other_statistics(task_df, worker_summary_df):
    #####################################################
    # General Statistics
    print("Generating category statistics...")
    # calculate the number of tasks submitted, ready, running, waiting_retrieval, retrieved, done
    for category, info in category_info.items():
        info['num_tasks'] = len(info['tasks'])
        info['total_task_execution_time(s)'] = round(sum(info['tasks_execution_time(s)']), 4)
        info['avg_task_execution_time(s)'] = round(info['total_task_execution_time(s)'] / info['num_tasks'], 4)
        info['max_task_execution_time(s)'] = max(info['tasks_execution_time(s)'])
        info['min_task_execution_time(s)'] = min(info['tasks_execution_time(s)'])
    category_info_df = pd.DataFrame.from_dict(category_info, orient='index')
    category_info_df.index.name = 'category'
    category_info_df.to_csv(os.path.join(dirname, 'category_info.csv'), index=True)
    #####################################################

    #####################################################
    # Add info into manager_info
    print("Generating manager_info.csv...")

    worker_connection_events_df = pd.concat([
        pd.DataFrame({'time': worker_summary_df['time_connected'], 'type': 'connect', 'worker_id': worker_summary_df['worker_id']}),
        pd.DataFrame({'time': worker_summary_df['time_disconnected'], 'type': 'disconnect', 'worker_id': worker_summary_df['worker_id']})
    ])
    worker_connection_events_df = worker_connection_events_df.sort_values('time')

    current_concurrent_workers = 0
    concurrent_workers_list = []
    worker_connection_events = []
    worker_connection_events.append((manager_info['time_start'], 0, 'manager_start', -1))
    for _, event in worker_connection_events_df.iterrows():
        if event['type'] == 'connect':
            current_concurrent_workers += 1
        else:
            current_concurrent_workers -= 1
        concurrent_workers_list.append(current_concurrent_workers)
    worker_connection_events_df['concurrent_workers'] = concurrent_workers_list
    worker_connection_events_df.to_csv(os.path.join(dirname, 'worker_concurrency.csv'), index=False)

    manager_info['max_concurrent_workers'] = max([x[1] for x in worker_connection_events])
    # a task may be submitted multiple times
    manager_info['tasks_submitted'] = len(task_info)
    manager_info['time_start_human'] = timestamp_to_datestring(manager_info['time_start'])[:22]
    manager_info['time_end_human'] = timestamp_to_datestring(manager_info['time_end'])[:22]
    # the max try_id in task_df
    manager_info['max_task_try_count'] = task_df['try_id'].max()
    manager_info_df = pd.DataFrame([manager_info])
    manager_info_df.to_csv(os.path.join(dirname, 'manager_info.csv'), index=False)
    #####################################################

def generate_library_summary():
    library_df = pd.DataFrame.from_dict(library_info, orient='index')
    library_df.to_csv(os.path.join(dirname, 'library_summary.csv'), index=False)


def generate_task_df():
    print("Generating task.csv...")
    task_df = pd.DataFrame.from_dict(task_info, orient='index')

    # ensure that the running time is not greater than the done time
    task_df['when_running'] = np.where(
        task_df['time_worker_start'].gt(0) & task_df['time_worker_start'].notna(),
        np.minimum(task_df['when_running'], task_df['time_worker_start']),
        task_df['when_running']
    )

    # set the schedule_id according to the when_running from 1 (except when_running is na)
    task_df['schedule_id'] = task_df['when_running'].rank(method='dense')
    task_df['schedule_id'] = task_df['schedule_id'].fillna(-1).astype(int)

    task_df['schedule_id'] = np.where(task_df['when_running'].isna(), np.nan, task_df['schedule_id'])
    
    task_df.to_csv(os.path.join(dirname, 'task.csv'), index=False)

    is_done = task_df['when_done'].notnull()
    is_failed_manager = task_df['when_running'].isnull() & task_df['when_ready'].notnull()
    is_failed_worker = task_df['when_running'].notnull() & task_df['when_done'].isnull()

    manager_info['tasks_done'] = is_done.sum()
    manager_info['tasks_failed_on_manager'] = is_failed_manager.sum()
    manager_info['tasks_failed_on_worker'] = is_failed_worker.sum()

    def calculate_total_size_of_files(files):
        return round(sum([file_info[file]['size(MB)'] for file in files]), 4)

    def handle_each_task(task):
        # assume that every task consumes 1 core as of now
        cores = task['core_id']
        if len(cores) == 0:
            return task
        task['core_id'] = cores[0]

        # if the when_next_ready is na, that means the manager exited before the task was ready, set it to the worker end time
        if pd.isna(task['when_next_ready']):
            worker = worker_info[task['worker_committed']]
            for i in range(len(worker['time_connected'])):
                if len(worker['time_disconnected']) != len(worker['time_connected']):
                    # worker is still connected
                    worker['time_disconnected'].append(manager_info['time_end'])
                else:
                    if worker['time_connected'][i] < task['when_running'] and worker['time_disconnected'][i] > task['when_running']:
                        task['when_next_ready'] = worker['time_disconnected'][i]
        
        # calculate the total size of input and output files
        task['size_input_files(MB)'] = calculate_total_size_of_files(task['input_files'])
        task['size_output_files(MB)'] = calculate_total_size_of_files(task['output_files'])
        # calculate the critical parent
        parents = []
        for input_file in task['input_files']:
            file_producers = file_info[input_file]['producers']
            if file_producers:
                parents.extend(file_producers)
        # find the critical input file
        shorted_waiting_time = 1e8
        for p in parents:
            # exclude recovery tasks
            # if task['is_recovery_task']:
            #    continue
            parent_task = task_info[(p, task_try_count[p])]
            time_period = task[task_start_timestamp] - parent_task[task_finish_timestamp]

            if time_period < 0:
                # it means that this input file is lost after this task is done and it is used as another task's input file
                continue
            if time_period < shorted_waiting_time:
                shorted_waiting_time = task[task_start_timestamp] - parent_task[task_finish_timestamp]
                task['critical_parent'] = int(p)
                task['critical_input_file'] = parent_task['output_files'][0]
                task['critical_input_file_wait_time'] = shorted_waiting_time
        
        if not pd.isna(task['when_done']) and pd.isna(task['when_output_fully_lost']):
            task['when_output_fully_lost'] = manager_info['time_end']

        return task
    
    # the concurrent tasks throughout the manager's lifetime skip if when_running is na
    scheduled_task_df = task_df.dropna(subset=['when_running'])
    task_starting_df = pd.DataFrame({
        'time': scheduled_task_df['when_running'],
        'task_id': scheduled_task_df['task_id'],
        'worker_id': scheduled_task_df['worker_id'],
        'category': scheduled_task_df['category'],
        'type': 1
    })

    # skip if when_waiting_retrieval is na, use when_next_ready instead
    task_ending_df = pd.DataFrame({
        'time': scheduled_task_df.apply(lambda row: row['when_waiting_retrieval'] if pd.notna(row['when_waiting_retrieval']) else row['when_next_ready'], axis=1).dropna(),
        'task_id': scheduled_task_df['task_id'],
        'worker_id': scheduled_task_df['worker_id'],
        'category': scheduled_task_df['category'],
        'type': -1
    })

    events_df = pd.concat([task_starting_df, task_ending_df]).sort_values('time')

    events_df = events_df.sort_values('time')
    events_df['concurrent_tasks'] = events_df['type'].cumsum()
    events_df.to_csv(os.path.join(dirname, 'task_concurrency.csv'), index=False)

    task_df[is_done].apply(handle_each_task, axis=1).to_csv(os.path.join(dirname, 'task_done.csv'), index=False)
    task_df[is_failed_manager].apply(handle_each_task, axis=1).to_csv(os.path.join(dirname, 'task_failed_on_manager.csv'), index=False)
    task_df[is_failed_worker].apply(handle_each_task, axis=1).to_csv(os.path.join(dirname, 'task_failed_on_worker.csv'), index=False)

    return task_df

def generate_worker_disk_usage():
    print("Generating worker_disk_usage.csv...")
    rows = []
    for worker_hash, worker in worker_info.items():
        worker_id = worker['worker_id']
        for filename, disk_update in worker['disk_update'].items():
            # Initial checks for disk update logs
            len_in = len(disk_update['when_stage_in'])
            len_out = len(disk_update['when_stage_out'])

            # Preparing row data
            for event_time, disk_increment in zip(disk_update['when_stage_in'] + disk_update['when_stage_out'],
                                                 [disk_update['size(MB)']] * len_in + [-disk_update['size(MB)']] * len_out):
                if event_time < manager_info['time_start']:
                    if abs(event_time - manager_info['time_start']) < 1:
                        # manager_info['time_start'] is more accurate
                        event_time = manager_info['time_start']
                    else:
                        print(f"Warning: disk update start time {event_time} of file {filename} on worker {worker_hash} is before manager start time {manager_info['time_start']}")
                        exit(1)
                rows.append({
                    'worker_hash': worker_hash,
                    'worker_id': worker_id,
                    'filename': filename,
                    'when_stage_in_or_out': event_time,
                    'size(MB)': disk_increment
                })

    worker_disk_usage_df = pd.DataFrame(rows)

    if not worker_disk_usage_df.empty:
        worker_disk_usage_df = worker_disk_usage_df[worker_disk_usage_df['when_stage_in_or_out'] > 0]
        worker_disk_usage_df.sort_values(by=['worker_id', 'when_stage_in_or_out'], ascending=[True, True], inplace=True)
        # normal worker disk usage
        worker_disk_usage_df['disk_usage(MB)'] = worker_disk_usage_df.groupby('worker_id')['size(MB)'].cumsum()
        worker_disk_usage_df['disk_usage(%)'] = worker_disk_usage_df['disk_usage(MB)'] / worker_disk_usage_df['worker_hash'].map(lambda x: worker_info[x]['disk(MB)'])
        # only consider the accumulated disk usage (exclude the stage-out files)
        worker_disk_usage_df['positive_size(MB)'] = worker_disk_usage_df['size(MB)'].apply(lambda x: x if x > 0 else 0)
        worker_disk_usage_df['disk_usage_accumulation(MB)'] = worker_disk_usage_df.groupby('worker_id')['positive_size(MB)'].cumsum()
        worker_disk_usage_df['disk_usage_accumulation(%)'] = worker_disk_usage_df['disk_usage_accumulation(MB)'] / worker_disk_usage_df['worker_hash'].map(lambda x: worker_info[x]['disk(MB)'])
        worker_disk_usage_df.drop('positive_size(MB)', axis=1, inplace=True)

        worker_disk_usage_df.to_csv(os.path.join(dirname, 'worker_disk_usage.csv'), index=False)

    return worker_disk_usage_df


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('log_dir', type=str, help='the target log directory')
    parser.add_argument('--execution-details-only', action='store_true', help='Only generate data for task execution details')
    args = parser.parse_args()

    dirname = os.path.join(args.log_dir, 'vine-logs')
    txn = os.path.join(dirname, 'transactions')
    debug = os.path.join(dirname, 'debug')
    taskgraph = os.path.join(dirname, 'taskgraph')
    daskvine_log = os.path.join(dirname, 'daskvine.log')

    parse_txn()

    if not args.execution_details_only:
        parse_taskgraph()
        parse_debug()

    parse_daskvine_log()

    task_df = generate_task_df()
    worker_disk_usage_df  = generate_worker_disk_usage()
    worker_summary_df = generate_worker_summary(worker_disk_usage_df)
    generate_other_statistics(task_df, worker_summary_df)

    # for function calls
    generate_library_summary()
