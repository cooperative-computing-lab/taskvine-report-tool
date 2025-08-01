import re
import json
from bitarray import bitarray


class WorkerInfo:
    def __init__(self, ip: str, port: int, connect_id: int):
        # basic info
        self.ip = ip
        self.port = port
        self.connect_id = connect_id

        self.id = None
        self.hash = None
        self.machine_name = None
        self.transfer_port = None
        self.cores = None
        self.gpus = None
        self.memory_mb = None
        self.disk_mb = None
        self.time_connected = []
        self.time_disconnected = []
        self.coremap = None
        self.is_checkpoint_worker = False

        # task info
        self.tasks_completed = []
        self.tasks_failed = []
        self.tasks_running = set()

        # active files or transfers, set of filenames
        self.active_files_or_transfers = set()

    def add_active_file_or_transfer(self, filename: str):
        # allow double adding the same filename because we add upon "puturl" and then the subsequent "cache-update"
        self.active_files_or_transfers.add(filename)

    def remove_active_file_or_transfer(self, filename: str):
        # allow double removing the same filename (is this correct?)
        self.active_files_or_transfers.discard(filename)

    def set_checkpoint_worker(self):
        self.is_checkpoint_worker = True

    def add_connection(self, timestamp: float):
        self.time_connected.append(float(timestamp))

    def add_disconnection(self, timestamp: float):
        self.time_disconnected.append(float(timestamp))
        assert len(self.time_connected) == len(self.time_disconnected)

    def set_hash(self, hash: str):
        # note that the hash can be different for the same worker
        # because the worker can be restarted with the same ip and port
        if self.hash and hash != self.hash:
            raise ValueError(f"hash mismatch for worker {self.ip}:{self.port}")
        self.hash = hash

    def set_machine_name(self, machine_name: str):
        if self.machine_name and machine_name != self.machine_name:
            raise ValueError(
                f"machine name mismatch for worker {self.ip}:{self.port}")
        self.machine_name = machine_name

    def set_transfer_port(self, transfer_port):
        transfer_port = int(transfer_port)
        if self.transfer_port and transfer_port != self.transfer_port:
            raise ValueError(f"transfer port mismatch for worker {self.ip}:{self.port}")
        self.transfer_port = transfer_port

    def run_task(self, task):
        assert self.coremap is not None
        cores_found = 0
        for i in range(1, len(self.coremap)):
            if self.coremap[i] == 0:
                self.coremap[i] = 1
                task.core_id.append(i)
                cores_found += 1
                if cores_found == task.cores_requested:
                    self.tasks_running.add(task.task_id)
                    return i
        return -1

    def reap_task(self, task):
        assert self.coremap is not None
        if task.task_id in self.tasks_running:
            self.tasks_running.remove(task.task_id)
        else:
            return
        for core_id in task.core_id:
            self.coremap[core_id] = 0
        # if the task is a library task, we need to reset the worker's coremap,
        # because functions can be retrieved later, and before that, a new library can be dispatched,
        # its functions can be dispatched to the same worker, so both new and old functions can
        # sit on the same worker, so we need to reset the coremap
        if task.is_library_task:
            self.reset_coremap()

    def get_worker_ip_port(self):
        return f"{self.ip}:{self.port}"

    def get_worker_key(self):
        return f"{self.ip}:{self.port}:{self.connect_id}"

    def set_cores(self, cores: int):
        if self.cores and cores != self.cores:
            # cores should always match at the moment
            print(f"Warning: worker {self.ip}:{self.port}, reported different cores than before {self.cores} -> {cores}")
            return
        self.cores = cores
        if not self.coremap:
            self.coremap = bitarray(self.cores + 1)
            self.coremap.setall(0)
        else:
            pass

    def count_cores_used(self):
        return self.coremap.count(1)

    def reset_coremap(self):
        self.coremap = bitarray(self.cores + 1)
        self.coremap.setall(0)

    def set_gpus(self, gpus: int):
        if self.gpus and gpus != self.gpus:
            print(f"Warning: worker {self.ip}:{self.port}, reported different gpus than before {self.gpus} -> {gpus}")
        self.gpus = gpus

    def set_memory_mb(self, memory_mb: int):
        if self.memory_mb and memory_mb != self.memory_mb:
            print(f"Warning: worker {self.ip}:{self.port}, reported different memory than before {self.memory_mb} -> {memory_mb}")
        self.memory_mb = memory_mb

    def set_disk_mb(self, disk_mb: int):
        if self.disk_mb and disk_mb != self.disk_mb:
            print(f"Warning: worker {self.ip}:{self.port}, reported different disk than before {self.disk_mb} -> {disk_mb}")
        self.disk_mb = disk_mb

    def to_json(self):
        return {
            "ip": self.ip,
            "port": self.port,
            "connect_id": self.connect_id,
            "id": self.id,
            "hash": self.hash,
            "worker_entry": self.get_worker_key(),
            "machine_name": self.machine_name,
            "transfer_port": self.transfer_port,
            "cores": self.cores,
            "gpus": self.gpus,
            "memory_mb": self.memory_mb,
            "disk_mb": self.disk_mb,
            "time_connected": self.time_connected[0],
            "time_disconnected": self.time_disconnected[0],
        }

    def print_info(self):
        print(json.dumps(self.to_json(), indent=2))

    @staticmethod
    def extract_ip_port_from_string(string):
        IP_PORT_PATTERN = re.compile(r"(\d+\.\d+\.\d+\.\d+):(\d+)")
        match = IP_PORT_PATTERN.search(string)
        if match:
            return match.group(1), int(match.group(2))
        return None, None
