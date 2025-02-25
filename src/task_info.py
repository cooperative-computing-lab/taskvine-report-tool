class TaskInfo:
    def __init__(self, task_id: int, task_try_id: int):
        # basic info
        self.task_id = task_id
        self.task_try_id = task_try_id
        self.category = None
        self.input_files = set()
        self.output_files = set()
        self.is_recovery_task = False
        self.exhausted_resources = False

        """
        typedef enum {
            VINE_RESULT_SUCCESS = 0,	                 /**< The task ran successfully, and its Unix exit code is given by @ref vine_task_get_exit_code */
            VINE_RESULT_INPUT_MISSING = 1,	             /**< The task cannot be run due to a missing input file **/
            VINE_RESULT_OUTPUT_MISSING = 2,              /**< The task ran but failed to generate a specified output file **/
            VINE_RESULT_STDOUT_MISSING = 4,              /**< The task ran but its stdout has been truncated **/
            VINE_RESULT_SIGNAL = 1 << 3,	             /**< The task was terminated with a signal **/
            VINE_RESULT_RESOURCE_EXHAUSTION = 2 << 3,    /**< The task used more resources than requested **/
            VINE_RESULT_MAX_END_TIME = 3 << 3,            /**< The task ran after the specified (absolute since epoch) end time. **/
            VINE_RESULT_UNKNOWN = 4 << 3,	              /**< The result could not be classified. **/
            VINE_RESULT_FORSAKEN = 5 << 3,	              /**< The task failed, but it was not a task error **/
            VINE_RESULT_MAX_RETRIES = 6 << 3,             /**< Currently unused. **/
            VINE_RESULT_MAX_WALL_TIME = 7 << 3,           /**< The task ran for more than the specified time (relative since running in a worker). **/
            VINE_RESULT_RMONITOR_ERROR = 8 << 3,          /**< The task failed because the monitor did not produce a summary report. **/
            VINE_RESULT_OUTPUT_TRANSFER_ERROR = 9 << 3,   /**< The task failed because an output could be transfered to the manager (not enough disk space, incorrect write permissions. */
            VINE_RESULT_FIXED_LOCATION_MISSING = 10 << 3, /**< The task failed because no worker could satisfy the fixed location input file requirements. */
            VINE_RESULT_CANCELLED = 11 << 3,	          /**< The task was cancelled by the caller. */
            VINE_RESULT_LIBRARY_EXIT = 12 << 3,	          /**< Task is a library that has terminated. **/
            VINE_RESULT_SANDBOX_EXHAUSTION = 13 << 3,     /**< The task used more disk than the allowed sandbox. **/
            VINE_RESULT_MISSING_LIBRARY = 14 << 3,        /**< The task is a function requiring a library that does not exist. */

            WORKER_DISCONNECTED = 15 << 3,                /**< The task failed because the worker disconnected. */
        } vine_result_t;
        """

        self.task_status = None

        self.exit_status = None
        self.output_length = None
        self.bytes_sent = None
        self.sandbox_used = None
        self.stdout_size_mb = None

        # time info
        self.when_ready = None
        # self.time_commit_start = None
        # self.time_commit_end = None
        self.when_running = None
        self.time_worker_start = None
        self.time_worker_end = None
        self.when_waiting_retrieval = None
        self.when_retrieved = None
        self.when_done = None
        self.when_failure_happens = None

        # worker info
        self.worker_ip, self.worker_port = None, None
        self.worker_id = None
        self.core_id = []       # a task can be assigned to multiple cores
        self.committed_worker_hash = None
        self.cores_requested = None
        self.gpus_requested = None
        self.memory_requested_mb = None
        self.disk_requested_mb = None
        self.execution_time = None

    def set_when_ready(self, when_ready):
        when_ready = float(when_ready)
        if self.when_ready and when_ready != self.when_ready:
            raise ValueError(f"when_ready mismatch for task {self.task_id}")
        self.when_ready = when_ready

    def set_when_running(self, when_running):
        when_running = float(when_running)
        if self.when_running and when_running != self.when_running:
            raise ValueError(f"when_running mismatch for task {self.task_id}")
        self.when_running = when_running

    def set_when_failure_happens(self, when_failure_happens):
        try:
            # it could be a float indicating that the task was failed
            when_failure_happens = float(when_failure_happens)
            if self.when_failure_happens and when_failure_happens != self.when_failure_happens:
                raise ValueError(f"when_failure_happens mismatch for task {self.task_id}")
            self.when_failure_happens = when_failure_happens
            if self.when_failure_happens < self.when_ready:
                assert self.when_failure_happens - self.when_ready < 1
                self.when_failure_happens = self.when_ready
        except:
            # it could be a string indicating that the task was not failed
            self.when_failure_happens = when_failure_happens
            pass

    def set_when_waiting_retrieval(self, when_waiting_retrieval):
        when_waiting_retrieval = float(when_waiting_retrieval)
        if self.when_waiting_retrieval and when_waiting_retrieval != self.when_waiting_retrieval:
            raise ValueError(f"when_waiting_retrieval mismatch for task {self.task_id}")
        self.when_waiting_retrieval = when_waiting_retrieval
        if not self.time_worker_end:
            return
        if self.when_waiting_retrieval < self.time_worker_end:
            assert self.when_waiting_retrieval - self.time_worker_end < 1
            self.when_waiting_retrieval = self.time_worker_end

    def set_when_retrieved(self, when_retrieved):
        when_retrieved = float(when_retrieved)
        if self.when_retrieved and when_retrieved != self.when_retrieved:
            raise ValueError(f"when_retrieved mismatch for task {self.task_id}")
        self.when_retrieved = when_retrieved
        if not self.time_worker_end:
            return
        if self.when_retrieved < self.time_worker_end:
            assert self.when_retrieved - self.time_worker_end < 1
            self.when_retrieved = self.time_worker_end

    def set_when_done(self, when_done):
        when_done = float(when_done)
        if self.when_done and when_done != self.when_done:
            raise ValueError(f"when_done mismatch for task {self.task_id}")
        self.when_done = when_done
        if self.when_done < self.when_retrieved:
            assert self.when_done - self.when_retrieved < 1
            self.when_done = self.when_retrieved

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
        task_status = int(task_status)
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
            raise ValueError(f"time_worker_start mismatch for task {self.task_id}, {self.time_worker_start} != {time_worker_start}")
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
        self.execution_time = round(float(self.time_worker_end) - float(self.time_worker_start), 2)

    def set_worker_ip_port(self, worker_ip, worker_port):
        if self.worker_ip and worker_ip != self.worker_ip:
            raise ValueError(f"worker_ip mismatch for task {self.task_id}: {self.worker_ip} != {worker_ip}")
        self.worker_ip = worker_ip
        if self.worker_port and worker_port != self.worker_port:
            raise ValueError(f"worker_port mismatch for task {self.task_id}: {self.worker_port} != {worker_port}")
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

    def print_info(self):
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

        print("when_ready: ", self.when_ready)
        print("when_running: ", self.when_running)
        print("time_worker_start: ", self.time_worker_start)
        print("time_worker_end: ", self.time_worker_end)
        print("when_waiting_retrieval: ", self.when_waiting_retrieval)
        print("when_retrieved: ", self.when_retrieved)
        print("when_done: ", self.when_done)
        print("when_failure_happens: ", self.when_failure_happens)

        print("worker_ip: ", self.worker_ip)
        print("worker_port: ", self.worker_port)
        print("cores_requested: ", self.cores_requested)
        print("gpus_requested: ", self.gpus_requested)
        print("memory_requested_mb: ", self.memory_requested_mb)
        print("disk_requested_mb: ", self.disk_requested_mb)
        print("execution_time: ", self.execution_time)
        print("\n")
