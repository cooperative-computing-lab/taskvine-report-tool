class TransferEvent:
    def __init__(self, source, destination, event, file_type, cache_level):
        self.source = source
        self.destination = destination

        # event describes how the transfer is created
        assert event in ["manager_put", "manager_get",
                         "task_created", "puturl", "puturl_now", "mini_task"]
        self.event = event

        self.time_start_stage_in = None
        self.time_stage_in = None
        self.time_stage_out = None
        self.eventual_state = None

        """
        typedef enum {
            VINE_FILE = 1,              /**< A file or directory present at the manager. **/
            VINE_URL,                   /**< A file obtained by downloading from a URL. */
            VINE_TEMP,                  /**< A temporary file created as an output of a task. */
            VINE_BUFFER,                /**< A file obtained from data in the manager's memory space. */
            VINE_MINI_TASK,             /**< A file obtained by executing a Unix command line. */
        } vine_file_type_t;
        """
        file_type = int(file_type)
        assert file_type in [1, 2, 3, 4, 5]
        self.file_type = file_type

        """
        typedef enum {
            VINE_CACHE_LEVEL_TASK = 0,     /**< Do not cache file at worker. (default) */
            VINE_CACHE_LEVEL_WORKFLOW = 1, /**< File remains in cache of worker until workflow ends. */
            VINE_CACHE_LEVEL_WORKER = 2,   /**< File remains in cache of worker until worker terminates. */
            VINE_CACHE_LEVEL_FOREVER = 3   /**< File remains at execution site when worker terminates. (use with caution) */
        } vine_cache_level_t;
        """

        cache_level = int(cache_level)
        assert cache_level in [0, 1, 2, 3]
        self.cache_level = cache_level

        self.penalty = None

    def set_eventual_state(self, eventual_state):
        assert eventual_state in ["pending", "cache_invalid", "cache_update", "worker_received",
                                  "manager_received", "worker_removed", "manager_removed", "unlink", "failed_to_return", "failed_to_send"]
        self.eventual_state = eventual_state

    def start_stage_in(self, time_start_stage_in, eventual_state):
        assert self.time_start_stage_in is None and self.time_stage_in is None and self.time_stage_out is None
        self.time_start_stage_in = float(time_start_stage_in)
        self.set_eventual_state(eventual_state)

    def stage_in(self, time_stage_in, eventual_state):
        assert self.time_start_stage_in is not None and self.time_stage_in is None and self.time_stage_out is None
        self.time_stage_in = float(time_stage_in)
        self.set_eventual_state(eventual_state)

    def stage_out(self, time_stage_out, eventual_state):
        assert self.time_start_stage_in is not None
        assert self.time_stage_out is None
        self.time_stage_out = float(time_stage_out)
        self.set_eventual_state(eventual_state)

    def print_info(self):
        print(f"source: {self.source}")
        print(f"destination: {self.destination}")
        print(f"time_start_stage_in: {self.time_start_stage_in}")
        print(f"event: {self.event}")
        print(f"file_type: {self.file_type}")
        print(f"cache_level: {self.cache_level}")
        print(f"time_stage_in: {self.time_stage_in}")
        print(f"time_stage_out: {self.time_stage_out}")
        print(f"eventual_state: {self.eventual_state}")
        print("\n")


class FileInfo:
    def __init__(self, filename, size_mb, timestamp):
        self.filename = filename
        self.size_mb = size_mb
        self.created_time = timestamp

        self.transfers = []

        self.consumers = set()
        self.producers = set()

        self.worker_retentions = {}      # key: worker_entry, value: list of [time_retention_start, time_retention_end]
 
    def prune_file_on_worker_entry(self, worker_entry, time_stage_out):
        for transfer in self.transfers:
            if transfer.time_stage_out:
                continue
            if transfer.time_start_stage_in > time_stage_out:
                continue
            if isinstance(transfer.destination, tuple) and transfer.destination == worker_entry:
                transfer.stage_out(time_stage_out, "worker_removed")
            if isinstance(transfer.source, tuple) and transfer.source == worker_entry:
                transfer.stage_out(time_stage_out, "worker_removed")

    def add_consumer(self, consumer_task):
        self.consumers.add((consumer_task.task_id, consumer_task.task_try_id))

    def is_consumer(self, consumer_task):
        return (consumer_task.task_id, consumer_task.task_try_id) in self.consumers

    def is_producer(self, producer_task):
        return (producer_task.task_id, producer_task.task_try_id) in self.producers

    def add_producer(self, producer_task):
        self.producers.add((producer_task.task_id, producer_task.task_try_id))

    def add_transfer(self, source, destination, event, file_type, cache_level):
        transfer_event = TransferEvent(source, destination, event, file_type, cache_level)
        self.transfers.append(transfer_event)
        return transfer_event

    def get_transfers_on_source(self, source, eventual_state=None):
        if eventual_state:
            return [transfer for transfer in self.transfers if transfer.source == source and transfer.eventual_state == eventual_state]
        else:
            return [transfer for transfer in self.transfers if transfer.source == source]

    def get_emitted_transfers(self):
        return len(self.transfers)

    def get_succeeded_transfers(self):
        return len([transfer for transfer in self.transfers if transfer.time_stage_in])

    def get_failed_transfers(self):
        return len([transfer for transfer in self.transfers if transfer.time_stage_in is None])

    def get_distinct_sources(self):
        sources = set()
        for transfer in self.transfers:
            sources.add(transfer.source)
        return list(sources)

    def get_distinct_destinations(self):
        destinations = set()
        for transfer in self.transfers:
            destinations.add(transfer.destination)
        return list(destinations)

    def cache_update(self, worker_entry, time_stage_in, file_type, file_cache_level):
        # check if the file was started staging in before the cache update
        has_started_staging_in = False
        time_stage_in = float(time_stage_in)

        for transfer in self.transfers:
            if transfer.destination != worker_entry:
                continue
            if transfer.time_stage_in:
                continue
            if transfer.time_stage_out:
                continue
            if transfer.time_start_stage_in > time_stage_in:
                continue
            transfer.stage_in(time_stage_in, "cache_update")
            has_started_staging_in = True

        # this means a task-created file
        if not has_started_staging_in:
            producer_task_name = f"{list(self.producers)[-1]}"
            transfer = self.add_transfer(producer_task_name, worker_entry, "task_created", file_type, file_cache_level)
            transfer.start_stage_in(time_stage_in, "pending")
            transfer.stage_in(time_stage_in, "cache_update")

    def unlink(self, worker_entry, time_stage_out):
        # this affects the incoming transfers on the destination worker
        for transfer in self.transfers:
            if transfer.destination != worker_entry:
                continue
            if transfer.time_stage_out:
                continue
            if transfer.time_start_stage_in > time_stage_out:
                continue
            transfer.stage_out(time_stage_out, "unlink")

    def cache_invalid(self, worker, time_stage_out):
        # this affects the incoming transfers on the destination worker
        for transfer in self.transfers:
            if transfer.destination != worker:
                continue
            if transfer.time_stage_out:
                continue
            if transfer.time_start_stage_in > time_stage_out:
                continue
            transfer.stage_out(time_stage_out, "cache_invalid")

    def set_size_mb(self, size_mb):
        size_mb = float(size_mb)
        if self.size_mb > 0 and size_mb > 0 and size_mb != self.size_mb:
            # it is normal that the same file is created multiple times with different sizes
            pass
        if size_mb > 0:
            self.size_mb = size_mb

    def set_penalty(self, penalty):
        self.penalty = penalty

    def print_info(self):
        print(f"filename: {self.filename}")
        print(f"size_mb: {self.size_mb}")
        print(f"consumers: {self.consumers}")
        print(f"producers: {self.producers}")
        print(f"penalty: {self.penalty}")
        print(f"transfers: {len(self.transfers)}")
        len_start_stage_in = len(
            [transfer for transfer in self.transfers if transfer.time_start_stage_in])
        len_stage_in = len(
            [transfer for transfer in self.transfers if transfer.time_stage_in])
        len_stage_out = len(
            [transfer for transfer in self.transfers if transfer.time_stage_out])
        print(f" len_start_stage_in: {len_start_stage_in}")
        print(f" len_stage_in: {len_stage_in}")
        print(f" len_stage_out: {len_stage_out}")
        print("\n")
