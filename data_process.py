import argparse
import os
import copy
import json
import pandas as pd
from datetime import datetime
import re
import time
from tqdm import tqdm
from collections import defaultdict
import json
from bitarray import bitarray # type: ignore
from tqdm import tqdm
import re
import cloudpickle
from datetime import datetime, timezone, timedelta
import pytz
import numpy as np
from worker_info import WorkerInfo
from task_info import TaskInfo
from file_info import FileInfo
from decimal import Decimal, ROUND_FLOOR


class DataProcessor:
    def __init__(self, workers, files, tasks, manager_info, csv_files_dir, json_files_dir):
        self.workers = workers
        self.files = files
        self.tasks = tasks
        self.manager_info = manager_info
        self.csv_files_dir = csv_files_dir
        self.json_files_dir = json_files_dir

        os.makedirs(csv_files_dir, exist_ok=True)
        os.makedirs(json_files_dir, exist_ok=True)

    def generate_data(self):
        # generate all the csv files
        self.generate_task_info_csv()
        self.generate_file_info_csv()
        self.generate_file_transfers_csv()
        self.generate_worker_info_csv()
    
        # generate all the json files
        self.generate_manager_info_json()

    def generate_task_info_csv(self):
        rows = []
        for task in self.tasks.values():
            # assume each task uses 1 core as of now
            assert len(task.core_id) == 1
            row = {
                'task_id': task.task_id,
                'task_try_id': task.task_try_id,
                'category': task.category,
                'input_files': list(task.input_files),
                'output_files': list(task.output_files),
                'is_recovery_task': task.is_recovery_task,
                'exhausted_resources': task.exhausted_resources,
                'task_status': task.task_status,
                'exit_status': task.exit_status,
                'output_length': task.output_length,
                'bytes_sent': task.bytes_sent,
                'sandbox_used': task.sandbox_used,
                'stdout_size_mb': task.stdout_size_mb,

                'when_ready': task.when_ready,
                'when_input_transfer_ready': task.when_input_transfer_ready,
                'time_commit_start': task.time_commit_start,
                'time_commit_end': task.time_commit_end,
                'when_running': task.when_running,
                'time_worker_start': task.time_worker_start,
                'time_worker_end': task.time_worker_end,
                'when_waiting_retrieval': task.when_waiting_retrieval,
                'when_retrieved': task.when_retrieved,
                'when_done': task.when_done,
                'when_next_ready': task.when_next_ready,

                'worker_ip': task.worker_ip,
                'worker_port': task.worker_port,
                'worker_id': self.workers[(task.worker_ip, task.worker_port)].id,
                'core_id': task.core_id[0],
                'committed_worker_hash': task.committed_worker_hash,
                'cores_requested': task.cores_requested,
                'gpus_requested': task.gpus_requested,
                'memory_requested_mb': task.memory_requested_mb,
                'disk_requested_mb': task.disk_requested_mb,
                'execution_time': task.execution_time,
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        df.to_csv(os.path.join(self.csv_files_dir, 'task_info.csv'), index=False)

    def generate_file_info_csv(self):
        rows = []
        for filename, file in self.files.items():
            row = {
                'filename': filename,
                'size_mb': file.size_mb,
                'consumers': file.consumers,
                'producers': file.producers,
                'num_emitted_transfers': file.get_emitted_transfers(),
                'num_succeeded_transfers': file.get_succeeded_transfers(),
                'num_failed_transfers': file.get_failed_transfers(),
                'num_distinct_sources': len(file.get_distinct_sources()),
                'num_distinct_destinations': len(file.get_distinct_destinations()),
            }
            rows.append(row)
        df = pd.DataFrame(rows)
        df.to_csv(os.path.join(self.csv_files_dir, 'file_info.csv'), index=False)

    def generate_file_transfers_csv(self):
        rows = []
        for filename, file in self.files.items():
            for transfer in file.transfers.values():
                # format the source and destination
                source = transfer.source
                if isinstance(source, tuple):
                    source = f"{source[0]}:{source[1]}"
                destination = transfer.destination
                if isinstance(destination, tuple):
                    destination = f"{destination[0]}:{destination[1]}"
                # the time_stage_in might be None
                time_stage_in = transfer.time_stage_in
                if time_stage_in is None:
                    time_stage_in = "None"
                row = {
                    'filename': filename,
                    'source': source,
                    'destination': destination,
                    'event': transfer.event,
                    'time_start_stage_in': transfer.time_start_stage_in,
                    'time_stage_in': time_stage_in,
                    'time_stage_out': transfer.time_stage_out,
                    'event': transfer.event,
                    'eventual_state': transfer.eventual_state,
                    'file_type': transfer.file_type,
                    'cache_level': transfer.cache_level,
                }
                rows.append(row)
        df = pd.DataFrame(rows)
        df.to_csv(os.path.join(self.csv_files_dir, 'file_transfers.csv'), index=False)

    def generate_manager_info_json(self):
        row = {
            'time_zone_offset_hours': self.manager_info.time_zone_offset_hours,
            'time_start': self.manager_info.time_start,
            'time_end': self.manager_info.time_end,
            'lifetime_s': self.manager_info.lifetime_s,
            'time_start_human': self.manager_info.time_start_human,
            'time_end_human': self.manager_info.time_end_human,
            'when_first_task_start_commit': self.manager_info.when_first_task_start_commit,
            'when_last_task_done': self.manager_info.when_last_task_done,
            'when_first_worker_connect': self.manager_info.when_first_worker_connect,
            'when_last_worker_disconnect': self.manager_info.when_last_worker_disconnect,
            'tasks_submitted': self.manager_info.tasks_submitted,
            'tasks_done': self.manager_info.tasks_done,
            'tasks_failed_on_manager': self.manager_info.tasks_failed_on_manager,
            'tasks_failed_on_worker': self.manager_info.tasks_failed_on_worker,
            'max_task_try_count': self.manager_info.max_task_try_count,
            'total_workers': self.manager_info.total_workers,
            'max_concurrent_workers': self.manager_info.max_concurrent_workers,
            'failed': self.manager_info.failed,
            'active_workers': self.manager_info.active_workers,
            'size_of_all_files_mb': self.manager_info.size_of_all_files_mb,
            'cluster_peak_disk_usage_mb': self.manager_info.cluster_peak_disk_usage_mb,
        }
        with open(os.path.join(self.json_files_dir, 'manager_info.json'), 'w') as f:
            json.dump(row, f, indent=4)

    def generate_worker_info_csv(self):
        rows = []
        for worker in self.workers.values():
            row = {
                'id': worker.id,
                'ip': worker.ip,
                'port': worker.port,
                'hash': worker.hash,
                'machine_name': worker.machine_name,
                'transfer_port': worker.transfer_port,
                'cores': worker.cores,
                'gpus': worker.gpus,
                'memory_mb': worker.memory_mb,
                'disk_mb': worker.disk_mb,
                'time_connected': worker.time_connected,
                'time_disconnected': worker.time_disconnected,
                'tasks_completed': len(worker.tasks_completed),
                'tasks_failed': len(worker.tasks_failed),
            }
            rows.append(row)
        df = pd.DataFrame(rows)
        df.to_csv(os.path.join(self.csv_files_dir, 'worker_info.csv'), index=False)