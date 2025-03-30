import re
from bitarray import bitarray


class WorkerInfo:
    def __init__(self, ip: str, port: int):
        # basic info
        self.id = None
        self.ip = ip
        self.port = port
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
        self.is_pbb = False

        # task info
        self.tasks_completed = []
        self.tasks_failed = []

    def add_connection(self, timestamp: float):
        self.time_connected.append(float(timestamp))

    def add_disconnection(self, timestamp: float):
        self.time_disconnected.append(float(timestamp))
        assert len(self.time_connected) == len(self.time_disconnected)

    def set_hash(self, hash: str):
        if self.hash and hash != self.hash:
            raise ValueError(f"hash mismatch for worker {self.ip}:{self.port}")
        self.hash = hash

    def enable_pbb(self):
        self.is_pbb = True

    def disable_pbb(self):
        self.is_pbb = False

    def set_machine_name(self, machine_name: str):
        if self.machine_name and machine_name != self.machine_name:
            raise ValueError(f"machine name mismatch for worker {self.ip}:{self.port}")
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
                    return i
        raise ValueError(f"not enough cores available for task {task.task_id}")
    
    def reap_task(self, task):
        assert self.coremap is not None
        for core_id in task.core_id:
            self.coremap[core_id] = 0
    
    def set_cores(self, cores: int):
        if self.cores and cores != self.cores:
            raise ValueError(f"cores mismatch for worker {self.ip}:{self.port}")
        self.cores = cores
        if not self.coremap:
            self.coremap = bitarray(cores + 1)
            self.coremap.setall(0)

    def set_gpus(self, gpus: int):
        if self.gpus and gpus != self.gpus:
            raise ValueError(f"gpus mismatch for worker {self.ip}:{self.port}")
        self.gpus = gpus

    def set_memory_mb(self, memory_mb: int):
        if self.memory_mb and memory_mb != self.memory_mb:
            raise ValueError(f"memory mismatch for worker {self.ip}:{self.port}")
        self.memory_mb = memory_mb

    def set_disk_mb(self, disk_mb: int):
        if self.disk_mb and disk_mb != self.disk_mb:
            raise ValueError(f"disk mismatch for worker {self.ip}:{self.port}")
        self.disk_mb = disk_mb
    
    def print_info(self):
        print("id: ", self.id)
        print("ip: ", self.ip)
        print("port: ", self.port)
        print("hash: ", self.hash)
        print("machine_name: ", self.machine_name)
        print("transfer_port: ", self.transfer_port)
        print("cores: ", self.cores)
        print("gpus: ", self.gpus)
        print("memory_mb: ", self.memory_mb)
        print("disk_mb: ", self.disk_mb)
        print("time_connected: ", self.time_connected)
        print("time_disconnected: ", self.time_disconnected)
        print("\n")
        
    @staticmethod
    def extract_ip_port_from_string(string):
        IP_PORT_PATTERN = re.compile(r"(\d+\.\d+\.\d+\.\d+):(\d+)")
        match = IP_PORT_PATTERN.search(string)
        if match:
            return match.group(1), int(match.group(2))
        return None, None