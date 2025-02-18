class TransferEvent:
    def __init__(self, source, destination, time_start_stage_in, event, file_type, cache_level):
        self.source = source
        self.destination = destination
        self.time_start_stage_in = time_start_stage_in
        
        # event describes how the transfer is created
        assert event in ["manager_put", "manager_get", "task_created", "puturl", "puturl_now"]
        self.event = event

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

    def set_time_stage_in(self, time_stage_in):
        assert self.time_stage_in is None
        if self.time_start_stage_in > time_stage_in:
            raise ValueError(f"time_stage_in {time_stage_in} is less than time_start_stage_in {self.time_start_stage_in}. {self.source} {self.destination} {self.event} {self.file_type} {self.cache_level}")
        self.time_stage_in = time_stage_in

    def set_time_stage_out(self, time_stage_out):
        assert self.time_stage_out is None
        self.time_stage_out = time_stage_out

    def set_eventual_state(self, eventual_state):
        assert eventual_state in ["pending", "cache_invalid", "cache_update", "worker_received", "manager_received", "worker_removed", "manager_removed", "unlink"]
        self.eventual_state = eventual_state

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
        print(f"\n")


class FileInfo:
    def __init__(self, filename, size_mb):
        self.filename = filename
        self.size_mb = size_mb

        self.transfers = {}        # key: (source, destination, time_start_stage_in), value: TransferEvent
        
        self.consumers = []
        self.producers = []

    def add_consumer(self, consumer):
        if consumer not in self.consumers:
            self.consumers.append(consumer)

    def add_producer(self, producer):
        if producer not in self.producers:
            self.producers.append(producer)

    def start_new_transfer(self, source, destination, time_start_stage_in, event, file_type, cache_level):
        transfer_event = TransferEvent(source, destination, time_start_stage_in, event, file_type, cache_level)
        transfer_event.set_eventual_state("pending")
        self.transfers[(source, destination, time_start_stage_in)] = transfer_event
        return transfer_event

    def get_transfer(self, source, destination, time_start_stage_in):
        return self.transfers[(source, destination, time_start_stage_in)]
    
    def get_transfers_on_source(self, source, eventual_state=None):
        if eventual_state:
            # find all transfers on source with a given eventual_state
            return [transfer for transfer in self.transfers.values() if transfer.source == source and transfer.eventual_state == eventual_state]
        else:
            # find all transfers on source
            return [transfer for transfer in self.transfers.values() if transfer.source == source]
    
    def get_transfers_on_destination(self, destination, eventual_state=None):
        if eventual_state:
            # find all transfers on destination with a given eventual_state
            return [transfer for transfer in self.transfers.values() if transfer.destination == destination and transfer.eventual_state == eventual_state]
        else:
            # find all transfers on destination
            return [transfer for transfer in self.transfers.values() if transfer.destination == destination]
    
    def get_emitted_transfers(self):
        return len(self.transfers)
    
    def get_succeeded_transfers(self):
        return len([transfer for transfer in self.transfers.values() if transfer.time_stage_in])
    
    def get_failed_transfers(self):
        return len([transfer for transfer in self.transfers.values() if transfer.time_stage_in is None])

    def get_worker_holdings(self):
        worker_holdings = []
        for transfer in self.transfers.values():
            if transfer.time_stage_in and transfer.time_stage_out:
                worker_holdings.append([transfer.destination, float(transfer.time_stage_in), float(transfer.time_stage_out), float(transfer.time_stage_out - transfer.time_stage_in)])
        return worker_holdings
    
    def get_distinct_sources(self):
        sources = set()
        for transfer in self.transfers.values():
            sources.add(transfer.source)
        return list(sources)
    
    def get_distinct_destinations(self):
        destinations = set()
        for transfer in self.transfers.values():
            destinations.add(transfer.destination)
        return list(destinations)

    def unlink_on_destination(self, time_stage_out, destination):
        transfers = self.get_transfers_on_destination(destination)
        for transfer in transfers:
            # skip if the transfer had been staged out
            if transfer.time_stage_out:
                continue
            # skip if the start time > time_stage_out
            if transfer.time_start_stage_in > time_stage_out:
                print(f"unlink {self.filename} {transfer.source} {transfer.destination} {transfer.time_start_stage_in} {time_stage_out}")
                continue
            # set the eventual_state to unlink
            transfer.set_eventual_state("unlink")
            transfer.set_time_stage_out(time_stage_out)

    def cache_invalid_on_destination(self, time_stage_out, destination):
        transfers = self.get_transfers_on_destination(destination)
        for transfer in transfers:
            # skip if the transfer had been staged out
            if transfer.time_stage_out:
                continue
            # skip if the start time > time_stage_out
            if transfer.time_start_stage_in > time_stage_out:
                continue
            # set the eventual_state to cache-invalid
            transfer.set_eventual_state("cache_invalid")
            transfer.set_time_stage_out(time_stage_out)
    
    def cache_update_transfers_on_destination(self, destination, time_stage_in):
        # find all transfers on destination
        transfers_on_destination = self.get_transfers_on_destination(destination)
        
        # if there are no transfers on destination, this means a task-created file
        if not transfers_on_destination:
            return

        # sort transfers by time_start_stage_in
        transfers_on_destination.sort(key=lambda x: x.time_start_stage_in)

        for transfer in transfers_on_destination:
            # if the transfer has not been received yet, set the received status to cache-update and set the time_stage_in
            if not transfer.time_stage_in and transfer.time_start_stage_in <= time_stage_in:
                transfer.set_eventual_state("cache_update")
                transfer.set_time_stage_in(time_stage_in)

    def manager_removed_on_destination(self, time_stage_out, destination):
        transfers = self.get_transfers_on_destination(destination)
        for transfer in transfers:
            # skip if the transfer had been staged out
            if transfer.time_stage_out:
                continue
            # skip if the start time > time_stage_out
            if transfer.time_start_stage_in > time_stage_out:
                continue
            transfer.set_eventual_state("manager_removed")
            transfer.set_time_stage_out(time_stage_out)

    def worker_removed_on_destination(self, time_stage_out, destination):
        transfers = self.get_transfers_on_destination(destination, eventual_state="pending")
        for transfer in transfers:
            transfer.set_eventual_state("worker_removed")
            transfer.set_time_stage_out(time_stage_out)

    def set_size_mb(self, size_mb):
        size_mb = float(size_mb)
        if self.size_mb > 0 and size_mb > 0 and size_mb != self.size_mb:
            raise ValueError(f"size mismatch for {self.filename}, {self.size_mb} != {size_mb}")
        if size_mb > 0:
            self.size_mb = size_mb

    def print_info(self):
        print(f"filename: {self.filename}")
        print(f"size_mb: {self.size_mb}")
        print(f"consumers: {self.consumers}")
        print(f"producers: {self.producers}")
        print(f"transfers: {len(self.transfers)}")
        len_start_stage_in = len([transfer for transfer in self.transfers.values() if transfer.time_start_stage_in])
        len_stage_in = len([transfer for transfer in self.transfers.values() if transfer.time_stage_in])
        len_stage_out = len([transfer for transfer in self.transfers.values() if transfer.time_stage_out])
        print(f" len_start_stage_in: {len_start_stage_in}")
        print(f" len_stage_in: {len_stage_in}")
        print(f" len_stage_out: {len_stage_out}")
        print(f"\n")
