from datetime import datetime, timezone, timedelta


class ManagerInfo:
    def __init__(self):
        self.ip = None
        self.port = None
        self.transfer_port = None

        # time info
        self.time_zone_offset_hours = None
        self.time_start = None
        self.time_end = None
        self.when_first_task_start_commit = None
        self.when_last_task_done = None
        self.tasks_submitted = None
        self.tasks_done = None
        self.tasks_failed_on_manager = None
        self.tasks_failed_on_worker = None
        self.max_task_try_count = None
        self.total_workers = None
        self.max_concurrent_workers = None
        self.failed = None
        self.active_workers = None
        self.when_first_worker_connect = None
        self.when_last_worker_disconnect = None
        self.size_of_all_files_mb = None
        self.cluster_peak_disk_usage_mb = None
        self.lifetime_s = None
        self.time_start_human = None
        self.time_end_human = None
        self.current_max_time = None
        self.equivalent_tz = None
        self.checkpoint_processing_time_us = 0

    def update_current_max_time(self, time):
        try:
            time = float(time)
        except Exception:
            return
        if self.current_max_time is None:
            self.current_max_time = time
        else:
            self.current_max_time = max(self.current_max_time, time)

    def set_time_start(self, time_start):
        self.time_start = float(time_start)
        self.time_start_human = self.timestamp_to_datestring(self.time_start)

    def set_time_end(self, time_end):
        self.time_end = float(time_end)
        self.lifetime_s = round(self.time_end - self.time_start, 2)
        self.time_end_human = self.timestamp_to_datestring(self.time_end)

    def timestamp_to_datestring(self, unix_timestamp):
        tz_custom = timezone(timedelta(hours=self.time_zone_offset_hours))
        datestring_custom = datetime.fromtimestamp(
            unix_timestamp, tz=tz_custom).strftime("%Y/%m/%d %H:%M:%S.%f")
        return datestring_custom

    def set_when_first_task_start_commit(self, when_first_task_start_commit):
        if self.when_first_task_start_commit is None:
            self.when_first_task_start_commit = when_first_task_start_commit
        else:
            self.when_first_task_start_commit = min(
                self.when_first_task_start_commit, when_first_task_start_commit)

    def set_when_last_task_done(self, when_last_task_done):
        when_last_task_done = round(float(when_last_task_done), 2)
        if self.when_last_task_done is None:
            self.when_last_task_done = when_last_task_done
        else:
            self.when_last_task_done = max(
                self.when_last_task_done, when_last_task_done)

    def set_when_first_worker_connect(self, when_first_worker_connect):
        when_first_worker_connect = round(float(when_first_worker_connect), 2)
        if self.when_first_worker_connect is None:
            self.when_first_worker_connect = when_first_worker_connect
        else:
            self.when_first_worker_connect = min(
                self.when_first_worker_connect, when_first_worker_connect)

    def update_when_last_worker_disconnect(self, when_last_worker_disconnect):
        when_last_worker_disconnect = round(
            float(when_last_worker_disconnect), 2)
        if self.when_last_worker_disconnect is None:
            self.when_last_worker_disconnect = when_last_worker_disconnect
        else:
            self.when_last_worker_disconnect = max(
                self.when_last_worker_disconnect, when_last_worker_disconnect)

    def aggregate_checkpoint_processing_time(self, time_us):
        self.checkpoint_processing_time_us += float(time_us)

    def print_info(self):
        print("Manager Info:")
        print(f"  IP: {self.ip}")
        print(f"  Port: {self.port}")
        print(f"  Transfer Port: {self.transfer_port}")
        print(f"  Time Zone Offset Hours: {self.time_zone_offset_hours}")
        print(f"  Time Start: {self.time_start}")
        print(f"  Time End: {self.time_end}")
        print(
            f"  When First Task Start Commit: {self.when_first_task_start_commit}")
        print(f"  When Last Task Done: {self.when_last_task_done}")
        print(f"  When First Worker Connect: {self.when_first_worker_connect}")
        print(
            f"  When Last Worker Disconnect: {self.when_last_worker_disconnect}")
        print(f"  Size of All Files MB: {self.size_of_all_files_mb}")
        print(
            f"  Cluster Peak Disk Usage MB: {self.cluster_peak_disk_usage_mb}")
        print(f"  Lifetime S: {self.lifetime_s}")
        print(f"  Time Start Human: {self.time_start_human}")
        print(f"  Time End Human: {self.time_end_human}")
        print(f"  Current Max Time: {self.current_max_time}")
        print(f"  Equivalent Time Zone: {self.equivalent_tz}")
        print(f"  Time Zone Offset Hours: {self.time_zone_offset_hours}")
