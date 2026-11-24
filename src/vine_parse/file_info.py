from collections import defaultdict


class TransferEvent():
    def __init__(self, file_name, dest_worker_entry, time_start_stage_in, size_mb=None):
        self.file_name = file_name
        self.dest_worker_entry = dest_worker_entry
        # Size belongs to this cache residency, not to the mutable FileInfo.
        # A cache name may be reused by a later generation with a new size.
        self.size_mb = float(size_mb) if size_mb is not None else None

        self.time_start_stage_in = time_start_stage_in
        self.time_stage_in = None
        self.time_stage_out = None
        self.end_reason = None

    def cache_update(self, time_stage_in, size_mb=None):
        # note that the cache-update might be received after the unlink, in this case we simply skip it
        if self.time_stage_out is not None:
            return
        self.time_stage_in = time_stage_in
        if size_mb is not None and float(size_mb) >= 0:
            self.size_mb = float(size_mb)

    def cache_invalid(self, time_stage_out):
        if self.time_stage_out is not None:
            return
        self.time_stage_out = time_stage_out
        self.end_reason = "cache_invalid"

    def unlink(self, time_stage_out, reason="unlink"):
        if self.time_stage_out is not None:
            return
        self.time_stage_out = time_stage_out
        self.end_reason = reason


class IndexedTransferEvent(TransferEvent):
    def __init__(self, file_name, dest_worker_entry, time_start_stage_in, transfer_id, source, size_mb=None):
        super().__init__(file_name, dest_worker_entry, time_start_stage_in, size_mb=size_mb)
        self.transfer_id = transfer_id
        self.source = source   # the source can be a url or an ip:port format


class UnindexedTransferEvent(TransferEvent):
    def __init__(self, file_name, dest_worker, time_start_stage_in, size_mb=None):
        super().__init__(file_name, dest_worker, time_start_stage_in, size_mb=size_mb)


class FileInfo():
    def __init__(self, filename, size_mb, timestamp):
        self.filename = filename
        self.size_mb = size_mb
        self.created_time = timestamp
        self.file_idx = None

        self.indexed_transfers = {}  # key: transfer_id, value: IndexedTransferEvent
        self.unindexed_transfers = defaultdict(list)  # key: dest_worker_entry, value: list of UnindexedTransferEvent

        self.consumers = set()
        self.producers = set()

        self.worker_retentions = {}      # key: worker_entry, value: list of [time_retention_start, time_retention_end]
    
    def get_flattened_transfers(self):
        all_transfers = []
        for transfer in self.indexed_transfers.values():
            all_transfers.append(transfer)
        for transfer_list in self.unindexed_transfers.values():
            all_transfers.extend(transfer_list)
        return all_transfers
    
    def unlink_all(self, timestamp, reason="manager_end"):
        for transfer in self.indexed_transfers.values():
            transfer.unlink(timestamp, reason=reason)
            assert transfer.time_stage_out is not None, f"Transfer {transfer.transfer_id} for file {self.filename} did not unlink properly"
        for transfer_list in self.unindexed_transfers.values():
            for transfer in transfer_list:
                transfer.unlink(timestamp, reason=reason)
                assert transfer.time_stage_out is not None, f"Transfer {transfer.transfer_id} for file {self.filename} did not unlink properly"

    def add_consumer(self, consumer_task):
        self.consumers.add((consumer_task.task_id, consumer_task.task_try_id))

    def is_consumer(self, consumer_task):
        return (consumer_task.task_id, consumer_task.task_try_id) in self.consumers

    def is_producer(self, producer_task):
        return (producer_task.task_id, producer_task.task_try_id) in self.producers

    def add_producer(self, producer_task):
        self.producers.add((producer_task.task_id, producer_task.task_try_id))

    def cache_update(self, worker, time_stage_in, transfer_id, size_mb):
        worker_entry = worker.worker_entry
        if transfer_id == 'X':
            # A put (or mini task) to this worker that is still in flight is what this cache-update completes,
            # also for a file some task produced: a task output later put to another worker is one residency there,
            # not a put plus a new task output.
            pending = [t for t in self.unindexed_transfers[worker_entry]
                       if t.time_stage_in is None and t.time_stage_out is None]
            if pending:
                for transfer in pending:
                    transfer.cache_update(time_stage_in, size_mb=size_mb)
            elif len(self.producers) > 0:
                new_transfer = UnindexedTransferEvent(
                    self.filename, worker_entry, time_stage_in, size_mb=size_mb
                )
                new_transfer.cache_update(time_stage_in, size_mb=size_mb)
                new_transfer.origin = "task_output"
                self.unindexed_transfers[worker_entry].append(new_transfer)
            else:
                for transfer in self.unindexed_transfers[worker_entry]:
                    transfer.cache_update(time_stage_in, size_mb=size_mb)
        else:
            # note: the transfer id can be stale from a previous manager
            if transfer_id not in self.indexed_transfers:
                print(f"Warning: transfer id {transfer_id} not found for file {self.filename} on worker {worker_entry}")
                return
            transfer = self.indexed_transfers[transfer_id]
            transfer.cache_update(time_stage_in, size_mb=size_mb)

        worker.add_active_file_or_transfer(self.filename)

    def cache_invalid(self, worker, time_stage_out, transfer_id):
        worker_entry = worker.worker_entry

        if transfer_id is None:
            for transfer in self.unindexed_transfers[worker_entry]:
                transfer.cache_invalid(time_stage_out)
        else:
            # note: the transfer id can be stale from a previous manager
            if transfer_id not in self.indexed_transfers:
                print(f"Warning: transfer id {transfer_id} not found for file {self.filename} on worker {worker_entry}")
                return
            transfer = self.indexed_transfers[transfer_id]
            transfer.cache_invalid(time_stage_out)

        worker.remove_active_file_or_transfer(self.filename)

    def unlink(self, worker, time_stage_out, reason="unlink"):
        worker_entry = worker.worker_entry

        for transfer in self.unindexed_transfers[worker_entry]:
            transfer.unlink(time_stage_out, reason=reason)
        for transfer in self.indexed_transfers.values():
            if transfer.dest_worker_entry == worker_entry:
                transfer.unlink(time_stage_out, reason=reason)

        worker.remove_active_file_or_transfer(self.filename)

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
        print("\n")


class CheckpointTransfer:
    """One receive of a worker's file into the Manager's data-service vault (vine-graph fork).

    Checkpoints are a transfer class of their own: they move data from a
    worker to the Manager, never into a worker cache, so they are counted
    apart from worker-bound transfers and never form a cache residency.
    """

    def __init__(self, checkpoint_id, file_name, source, source_worker_entry, size_bytes, inline, required, urgent,
                 connect_back, time_start):
        self.checkpoint_id = checkpoint_id
        self.file_name = file_name
        self.source = source                            # "<transfer host>:<transfer port>" as logged
        self.source_worker_entry = source_worker_entry  # None when the address names no known worker
        self.size_bytes = size_bytes
        self.inline = inline                            # received over the control link, not a connection
        self.required = required
        self.urgent = urgent
        self.connect_back = connect_back
        self.time_start = time_start
        self.time_end = None
        self.outcome = None                             # complete, failed, or cancelled
