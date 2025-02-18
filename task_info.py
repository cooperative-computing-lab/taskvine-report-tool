class TaskInfo:
    def __init__(self, task_id: int, task_try_id: int, manager_info):
        self.manager_info = manager_info

        # basic info
        self.task_id = task_id
        self.task_try_id = task_try_id
        self.category = None
        self.input_files = set()
        self.output_files = set()
        self.is_recovery_task = False
        self.exhausted_resources = False
        self.task_status = None
        self.exit_status = None
        self.output_length = None
        self.bytes_sent = None
        self.sandbox_used = None
        self.stdout_size_mb = None

        # time info
        self.when_ready = None
        self.when_input_transfer_ready = None
        self.time_commit_start = None
        self.time_commit_end = None
        self.when_running = None
        self.time_worker_start = None
        self.time_worker_end = None
        self.when_waiting_retrieval = None
        self.when_retrieved = None
        self.when_done = None
        self.when_next_ready = None
        self.when_output_fully_lost = None

        # worker info
        self.worker_ip, self.worker_port = None, None
        self.core_id = []       # a task can be assigned to multiple cores
        self.committed_worker_hash = None
        self.cores_requested = None
        self.gpus_requested = None
        self.memory_requested_mb = None
        self.disk_requested_mb = None
        self.execution_time = None

    def set_done_code(self, done_code):
        if self.done_code and done_code != self.done_code:
            raise ValueError(f"done_code mismatch for task {self.task_id}")
        self.done_code = done_code

    def set_output_length(self, output_length):
        if self.output_length and output_length != self.output_length:
            raise ValueError(f"output_length mismatch for task {self.task_id}")
        self.output_length = output_length

    def set_bytes_sent(self, bytes_sent):
        if self.bytes_sent and bytes_sent != self.bytes_sent:
            raise ValueError(f"bytes_sent mismatch for task {self.task_id}")
        self.bytes_sent = bytes_sent

    def set_sandbox_used(self, sandbox_used):
        if self.sandbox_used and sandbox_used != self.sandbox_used:
            raise ValueError(f"sandbox_used mismatch for task {self.task_id}")
        self.sandbox_used = sandbox_used

    def set_task_status(self, task_status):
        if self.task_status and task_status != self.task_status:
            raise ValueError(f"task_status mismatch for task {self.task_id}")
        self.task_status = task_status

    def set_stdout_size_mb(self, stdout_size_mb):
        if self.stdout_size_mb and stdout_size_mb != self.stdout_size_mb:
            raise ValueError(f"stdout_size_mb mismatch for task {self.task_id}")
        self.stdout_size_mb = stdout_size_mb

    def set_time_worker_start(self, time_worker_start):
        time_worker_start = float(time_worker_start)
        if self.time_worker_start and time_worker_start != self.time_worker_start:
            raise ValueError(f"time_worker_start mismatch for task {self.task_id}")
        self.time_worker_start = time_worker_start

    def set_exit_status(self, exit_status):
        if self.exit_status and exit_status != self.exit_status:
            raise ValueError(f"exit_status mismatch for task {self.task_id}")
        self.exit_status = exit_status

    def set_time_worker_end(self, time_worker_end):
        time_worker_end = float(time_worker_end)
        if self.time_worker_end and time_worker_end != self.time_worker_end:
            raise ValueError(f"time_worker_end mismatch for task {self.task_id}")
        self.time_worker_end = time_worker_end
        self.execution_time = self.time_worker_end - self.time_worker_start

    def set_worker_ip_port(self, worker_ip, worker_port):
        if self.worker_ip and worker_ip != self.worker_ip:
            raise ValueError(f"worker_ip mismatch for task {self.task_id}")
        self.worker_ip = worker_ip
        if self.worker_port and worker_port != self.worker_port:
            raise ValueError(f"worker_port mismatch for task {self.task_id}")
        self.worker_port = worker_port

    def set_cores_requested(self, cores_requested):
        if self.cores_requested and cores_requested != self.cores_requested:
            raise ValueError(f"cores_requested mismatch for task {self.task_id}")
        self.cores_requested = cores_requested

    def set_gpus_requested(self, gpus_requested):
        if self.gpus_requested and gpus_requested != self.gpus_requested:
            raise ValueError(f"gpus_requested mismatch for task {self.task_id}")
        self.gpus_requested = gpus_requested

    def set_memory_requested_mb(self, memory_requested_mb):
        if self.memory_requested_mb and memory_requested_mb != self.memory_requested_mb:
            raise ValueError(f"memory_requested_mb mismatch for task {self.task_id}")
        self.memory_requested_mb = memory_requested_mb

    def set_disk_requested_mb(self, disk_requested_mb):
        if self.disk_requested_mb and disk_requested_mb != self.disk_requested_mb:
            raise ValueError(f"disk_requested_mb mismatch for task {self.task_id}")
        self.disk_requested_mb = disk_requested_mb

    def add_input_file(self, input_file):
        self.input_files.add(input_file)

    def add_output_file(self, output_file):
        self.output_files.add(output_file)

    def set_category(self, category):
        if self.category and category != self.category:
            raise ValueError(f"category mismatch for task {self.task_id}")
        self.category = category

    def print_task_info(self):
        print("task_id: ", self.task_id)
        print("task_try_id: ", self.task_try_id)
        print("category: ", self.category)
        print("input_files: ", self.input_files)
        print("output_files: ", self.output_files)
        print("is_recovery_task: ", self.is_recovery_task)
        print("exhausted_resources: ", self.exhausted_resources)
        print("task_status: ", self.task_status)
        print("exit_status: ", self.exit_status)
        print("output_length: ", self.output_length)
        print("bytes_sent: ", self.bytes_sent)
        print("sandbox_used: ", self.sandbox_used)
        print("stdout_size_mb: ", self.stdout_size_mb)
        print("time_worker_start: ", self.time_worker_start)
        print("time_worker_end: ", self.time_worker_end)
        print("when_waiting_retrieval: ", self.when_waiting_retrieval)
        print("when_retrieved: ", self.when_retrieved)
        print("when_done: ", self.when_done)
        print("when_next_ready: ", self.when_next_ready)
        print("when_output_fully_lost: ", self.when_output_fully_lost)
        print("worker_ip: ", self.worker_ip)
        print("worker_port: ", self.worker_port)
        print("cores_requested: ", self.cores_requested)
        print("gpus_requested: ", self.gpus_requested)
        print("memory_requested_mb: ", self.memory_requested_mb)
        print("disk_requested_mb: ", self.disk_requested_mb)
        print("execution_time: ", self.execution_time)
        print("\n")
