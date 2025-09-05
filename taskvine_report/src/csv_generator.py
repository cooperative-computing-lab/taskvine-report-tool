from .data_parser import DataParser
from collections import defaultdict
from taskvine_report.utils import *


class CSVManager:
    def __init__(self, runtime_template,
                 data_parser=None,
                 downsampling=True,
                 downsample_task_count=10000,
                 downsample_point_count=1000):
        self.runtime_template = runtime_template
        if not self.runtime_template:
            return

        self.downsampling = downsampling
        self.downsample_task_count = downsample_task_count if self.downsampling else sys.maxsize
        self.downsample_point_count = downsample_point_count if self.downsampling else sys.maxsize

        # csv files
        self.csv_files_dir = os.path.join(self.runtime_template, 'csv-files')
        ensure_dir(self.csv_files_dir, replace=False)
        self.csv_file_time_domain = os.path.join(self.csv_files_dir, 'time_domain.csv')
        self.csv_file_file_concurrent_replicas = os.path.join(self.csv_files_dir, 'file_concurrent_replicas.csv')
        self.csv_file_metadata = os.path.join(self.csv_files_dir, 'metadata.csv')
        self.csv_file_file_created_size = os.path.join(self.csv_files_dir, 'file_created_size.csv')
        self.csv_file_file_transferred_size = os.path.join(self.csv_files_dir, 'file_transferred_size.csv')
        self.csv_file_worker_concurrency = os.path.join(self.csv_files_dir, 'worker_concurrency.csv')
        self.csv_file_retention_time = os.path.join(self.csv_files_dir, 'file_retention_time.csv')
        self.csv_file_task_execution_time = os.path.join(self.csv_files_dir, 'task_execution_time.csv')
        self.csv_file_task_response_time = os.path.join(self.csv_files_dir, 'task_response_time.csv')
        self.csv_file_task_concurrency = os.path.join(self.csv_files_dir, 'task_concurrency.csv')
        self.csv_file_task_concurrency_recovery_only = os.path.join(self.csv_files_dir, 'task_concurrency_recovery_only.csv')
        self.csv_file_task_retrieval_time = os.path.join(self.csv_files_dir, 'task_retrieval_time.csv')
        self.csv_file_task_dependencies = os.path.join(self.csv_files_dir, 'task_dependencies.csv')
        self.csv_file_task_dependents = os.path.join(self.csv_files_dir, 'task_dependents.csv')
        self.csv_file_task_completion_percentiles = os.path.join(self.csv_files_dir, 'task_completion_percentiles.csv')
        self.csv_file_sizes = os.path.join(self.csv_files_dir, 'file_sizes.csv')
        self.csv_file_worker_lifetime = os.path.join(self.csv_files_dir, 'worker_lifetime.csv')
        self.csv_file_worker_executing_tasks = os.path.join(self.csv_files_dir, 'worker_executing_tasks.csv')
        self.csv_file_worker_waiting_retrieval_tasks = os.path.join(self.csv_files_dir, 'worker_waiting_retrieval_tasks.csv')
        self.csv_file_worker_incoming_transfers = os.path.join(self.csv_files_dir, 'worker_incoming_transfers.csv')
        self.csv_file_worker_outgoing_transfers = os.path.join(self.csv_files_dir, 'worker_outgoing_transfers.csv')
        self.csv_file_worker_storage_consumption = os.path.join(self.csv_files_dir, 'worker_storage_consumption.csv')
        self.csv_file_worker_storage_consumption_percentage = os.path.join(self.csv_files_dir, 'worker_storage_consumption_percentage.csv')
        self.csv_file_task_subgraphs = os.path.join(self.csv_files_dir, 'task_subgraphs.csv')
        self.csv_file_task_execution_details = os.path.join(self.csv_files_dir, 'task_execution_details.csv')
        self.csv_file_file_replica_activation_intervals = os.path.join(self.csv_files_dir, 'file_replica_activation_intervals.csv')

        self.dp = data_parser
        if self.dp:
            assert self.runtime_template == self.dp.runtime_template
            self.MIN_TIME, self.MAX_TIME = self.dp.manager.get_min_max_time()
            if self.MAX_TIME and self.MIN_TIME:
                self.time_domain = [0, self.MAX_TIME - self.MIN_TIME]
                df = pd.DataFrame({
                    'MIN_TIME': [self.MIN_TIME],
                    'MAX_TIME': [self.MAX_TIME]
                })
                write_df_to_csv(df, self.csv_file_time_domain, index=False)

    def generate_csv_files(self):
        # return if no tasks were dispatched
        if not self.MIN_TIME:
            return

        with create_progress_bar() as progress:
            task_id = progress.add_task("[green]Generating plotting data", total=7)

            self.generate_metadata()
            progress.advance(task_id)

            self.generate_file_metrics()
            progress.advance(task_id)

            self.generate_task_metrics()
            progress.advance(task_id)

            self.generate_task_concurrency_data()
            progress.advance(task_id)

            self.generate_task_execution_details_metrics()
            progress.advance(task_id)

            self.generate_worker_metrics()
            progress.advance(task_id)
            
            self.generate_subgraphs_and_graph_metrics()
            progress.advance(task_id)

    def add_workflow_completion_percentage(self, df):
        def map_fn(t):
            if t < 0:
                return 0
            denom = self.MAX_TIME - self.MIN_TIME
            if denom <= 0:
                return 0
            return t / denom * 100
        
        assert "time" in df.columns, "time column is required"
        df = df.with_columns(
            pl.col("time").map_elements(map_fn, return_dtype=pl.Float64)
            .alias("workflow_completion_percentage")
        )
        cols = df.columns
        reordered = ["time", "workflow_completion_percentage"] + [c for c in cols if c not in ("time", "workflow_completion_percentage")]
        return df.select(reordered)

    def generate_file_metrics(self):
        base_time = self.MIN_TIME

        rows_file_concurrent_replicas = []
        rows_file_created_size = []
        rows_file_transferred_size = []
        rows_file_retention_time = []
        rows_sizes = []
        max_size = 0

        all_worker_storage = defaultdict(list)
        rows_worker_transfer_events = {
            'incoming': defaultdict(list),
            'outgoing': defaultdict(list)
        }

        for file in self.dp.files.values():
            flattened_transfers = file.get_flattened_transfers()
            if not flattened_transfers:
                continue

            for transfer in flattened_transfers:
                # file transferred size
                if transfer.time_stage_in:
                    t = floor_decimal(float(transfer.time_stage_in - base_time), 2)
                    rows_file_transferred_size.append((t, file.size_mb))
                elif transfer.time_stage_out:
                    t = floor_decimal(float(transfer.time_stage_out - base_time), 2)
                    rows_file_transferred_size.append((t, file.size_mb))

                # worker storage consumption
                if transfer.time_stage_in:
                    time_in = floor_decimal(transfer.time_stage_in - base_time, 2)
                    time_out = floor_decimal(transfer.time_stage_out - base_time, 2)
                    size = max(0, file.size_mb)
                    all_worker_storage[transfer.dest_worker_entry].extend([(time_in, size), (time_out, -size)])

                # worker incoming / outgoing transfers
                for role in ['incoming', 'outgoing']:
                    if role == "incoming":
                        wid = transfer.dest_worker_entry
                    else:
                        wid = getattr(transfer, 'source', None)
                        if not isinstance(wid, tuple):
                            wid = None
                    if wid is None:
                        continue

                    t0 = floor_decimal(transfer.time_start_stage_in - base_time, 2)
                    t1 = None
                    if transfer.time_stage_in is not None:
                        t1 = floor_decimal(transfer.time_stage_in - base_time, 2)
                    elif transfer.time_stage_out is not None:
                        t1 = floor_decimal(transfer.time_stage_out - base_time, 2)
                    if t1 is not None and t1 >= t0:
                        rows_worker_transfer_events[role][wid].extend([(t0, 1), (t1, -1)])

            if not file.producers:
                continue
            intervals = [
                (t.time_stage_in, t.time_stage_out)
                for t in flattened_transfers
                if t.time_stage_in and t.time_stage_out
            ]
            max_simul = max_interval_overlap(intervals)
            rows_file_concurrent_replicas.append((file.file_idx, file.filename, max_simul, file.created_time))

            stage_times = np.array([t.time_stage_in for t in flattened_transfers if t.time_stage_in is not None])
            if stage_times.size > 0:
                t = np.floor((stage_times.min() - base_time) * 100) / 100
                rows_file_created_size.append((t, file.size_mb))

            times = np.array([t.time_start_stage_in for t in flattened_transfers if t.time_start_stage_in is not None])
            first_stage_in = times.min() if times.size > 0 else float('inf')
            last_stage_out = max((t.time_stage_out for t in flattened_transfers if t.time_stage_out), default=float('-inf'))
            if first_stage_in != float('inf') and last_stage_out != float('-inf'):
                retention_time = floor_decimal(last_stage_out - first_stage_in, 2)
                rows_file_retention_time.append((file.file_idx, file.filename, retention_time, file.created_time))

            fname = file.filename
            size = file.size_mb
            if size is not None:
                created_time = min((t.time_start_stage_in for t in flattened_transfers if t.time_start_stage_in), default=float('inf'))
                if created_time != float('inf'):
                    rows_sizes.append((file.file_idx, fname, size, created_time))
                    max_size = max(max_size, size)

        def _process_rows_file_concurrent_replicas(rows_file_concurrent_replicas):
            if not rows_file_concurrent_replicas:
                return pl.DataFrame({
                    'file_idx': [],
                    'file_name': [],
                    'max_simul_replicas': [],
                })
            df = pl.DataFrame(rows_file_concurrent_replicas, schema=['file_idx', 'file_name', 'max_simul_replicas', 'created_time'], orient="row")
            downsampled_df = downsample_df_polars(
                df.select(['file_idx', 'max_simul_replicas']),
                y_col='max_simul_replicas',
                downsample_point_count=self.downsample_point_count
            )
            df = downsampled_df.join(
                df.select(['file_idx', 'file_name']),
                on='file_idx',
                how='left'
            )
            return df.select(['file_idx', 'file_name', 'max_simul_replicas'])
        write_df_to_csv(_process_rows_file_concurrent_replicas(rows_file_concurrent_replicas), self.csv_file_file_concurrent_replicas)

        def _process_rows_file_created_size(rows_file_created_size):
            if not rows_file_created_size:
                return pl.DataFrame({
                    'time': [],
                    'delta_size_mb': [],
                })
            df = pl.DataFrame(rows_file_created_size, schema=['time', 'delta_size_mb'], orient="row")
            df = df.with_columns(pl.col('time').round(2))
            df = df.group_by('time').agg(pl.col('delta_size_mb').sum()).sort('time')
            df = df.with_columns(pl.col('delta_size_mb').cum_sum().clip(0).alias('cumulative_size_mb'))
            downsampled_df = downsample_df_polars(
                df.select(['time', 'cumulative_size_mb']),
                y_col='cumulative_size_mb',
                downsample_point_count=self.downsample_point_count
            )
            return downsampled_df.select(['time', 'cumulative_size_mb'])
        write_df_to_csv(_process_rows_file_created_size(rows_file_created_size), self.csv_file_file_created_size)

        def _process_rows_file_transferred_size(rows_file_transferred_size):
            if not rows_file_transferred_size:
                return pl.DataFrame({
                    'time': [],
                    'delta_size_mb': [],
                })
            df = pl.DataFrame(rows_file_transferred_size, schema=['time', 'delta_size_mb'], orient="row")
            df = df.with_columns(pl.col('time').round(2))
            df = df.group_by('time').agg(pl.col('delta_size_mb').sum()).sort('time')
            df = df.with_columns(pl.col('delta_size_mb').cum_sum().clip(0).alias('cumulative_size_mb'))
            downsampled_df = downsample_df_polars(
                df.select(['time', 'cumulative_size_mb']),
                y_col='cumulative_size_mb',
                downsample_point_count=self.downsample_point_count
            )
            return downsampled_df.select(['time', 'cumulative_size_mb'])
        write_df_to_csv(_process_rows_file_transferred_size(rows_file_transferred_size), self.csv_file_file_transferred_size)

        def _process_rows_file_retention_time(rows_file_retention_time):
            if not rows_file_retention_time:
                return pl.DataFrame({
                    'file_idx': [],
                    'file_name': [],
                    'retention_time': [],
                })
            df = pl.DataFrame(rows_file_retention_time, schema=['file_idx', 'file_name', 'retention_time', 'created_time'], orient="row")
            downsampled_df = downsample_df_polars(
                df.select(['file_idx', 'retention_time']),
                y_col='retention_time',
                downsample_point_count=self.downsample_point_count
            )
            df = downsampled_df.join(
                df.select(['file_idx', 'file_name']),
                on='file_idx',
                how='left'
            )
            return df.select(['file_idx', 'file_name', 'retention_time'])
        write_df_to_csv(_process_rows_file_retention_time(rows_file_retention_time), self.csv_file_retention_time)
        
        def _process_rows_file_sizes(rows_sizes):
            if not rows_sizes:
                return pl.DataFrame({
                    'file_idx': [],
                    'file_name': [],
                    'file_size': [],
                })
            df = pl.DataFrame(rows_sizes, schema=['file_idx', 'file_name', 'file_size', 'created_time'], orient="row")
            downsampled_df = downsample_df_polars(
                df.select(['file_idx', 'file_size']),
                y_col='file_size',
                downsample_point_count=self.downsample_point_count
            )
            df = downsampled_df.join(
                df.select(['file_idx', 'file_name']),
                on='file_idx',
                how='left'
            )
            max_size = df['file_size'].max()
            unit, scale = get_size_unit_and_scale(max_size)
            df = df.with_columns((pl.col('file_size') * scale).alias(f'file_size_{unit.lower()}'))
            return df.select(['file_idx', 'file_name', f'file_size_{unit.lower()}'])
        write_df_to_csv(_process_rows_file_sizes(rows_sizes), self.csv_file_sizes)

        def _process_rows_worker_transfers(rows_worker_transfer_events):

            if not rows_worker_transfer_events:
                return pl.DataFrame({
                    'time': [],
                    'cumulative': [],
                })
            
            col_data = {}
            all_times = set()
            for wid, events in rows_worker_transfer_events.items():
                if not events:
                    continue

                arr = np.asarray(events, dtype=np.float64)
                arr = arr[arr[:, 0].argsort()]
                times, idx = np.unique(arr[:, 0], return_inverse=True)
                delta = np.bincount(idx, weights=arr[:, 1], minlength=len(times))
                cumulative = np.clip(np.cumsum(delta), 0, None)
                df = pl.DataFrame({'time': times, 'cumulative': cumulative})
                if df.height > self.downsample_point_count:
                    df = downsample_df_polars(df, y_col='cumulative', downsample_point_count=self.downsample_point_count)
                key = f"{wid[0]}:{wid[1]}:{wid[2]}"
                col_data[key] = {float(row[0]): float(row[1]) for row in df.iter_rows()}
                all_times.update(col_data[key].keys())
            if not col_data:
                return pl.DataFrame({
                    'time': [],
                    'cumulative': [],
                })
            sorted_times = sorted(all_times)
            out_df = pl.DataFrame({'time': sorted_times})
            for key in sorted(col_data):
                values = [col_data[key].get(t, None) for t in sorted_times]
                out_df = out_df.with_columns(pl.Series(name=key, values=values))
            return out_df
        write_df_to_csv(_process_rows_worker_transfers(rows_worker_transfer_events['incoming']), self.csv_file_worker_incoming_transfers)
        write_df_to_csv(_process_rows_worker_transfers(rows_worker_transfer_events['outgoing']), self.csv_file_worker_outgoing_transfers)

        def _process_rows_worker_storage_consumption(rows_worker_storage_consumption, workers=None, percentage=False):
            col_data = {}
            all_times = set()
            for wid, events in rows_worker_storage_consumption.items():
                arr = np.asarray(events, dtype=np.float64)
                if arr.shape[0] == 0:
                    continue
                arr = arr[arr[:, 0].argsort()]
                times, idx = np.unique(arr[:, 0], return_inverse=True)
                delta = np.bincount(idx, weights=arr[:, 1], minlength=len(times))
                cumulative = np.clip(np.cumsum(delta), 0, None)
                if percentage and workers is not None:
                    worker = workers.get(wid)
                    if not worker or worker.disk_mb <= 0:
                        continue
                    cumulative = cumulative / worker.disk_mb * 100
                df = pl.DataFrame({'time': times, 'cumulative': cumulative})
                if df.height > self.downsample_point_count:
                    df = downsample_df_polars(df, y_col='cumulative', downsample_point_count=self.downsample_point_count)
                key = f"{wid[0]}:{wid[1]}:{wid[2]}"
                col_data[key] = {float(row[0]): float(row[1]) for row in df.iter_rows()}
                all_times.update(col_data[key].keys())

            # Ensure zero storage at connection boundary times per worker: time_connected[0] and time_disconnected[0], applied last
            if workers is not None:
                for w_key, w in workers.items():
                    key = f"{w_key[0]}:{w_key[1]}:{w_key[2]}"
                    if key not in col_data:
                        col_data[key] = {}
                    assert len(w.time_connected) == 1
                    assert len(w.time_disconnected) == 1
                    t0 = floor_decimal(float(w.time_connected[0] - base_time), 2)
                    col_data[key][float(t0)] = 0.0
                    all_times.add(float(t0))
                    t1 = floor_decimal(float(w.time_disconnected[0] - base_time), 2)
                    col_data[key][float(t1)] = 0.0
                    all_times.add(float(t1))

            sorted_times = sorted(all_times)
            out_df = pl.DataFrame({'time': sorted_times})

            # filter out negative time
            out_df = out_df.filter(pl.col("time") >= 0)

            if col_data:
                worker_data = {
                    key: [col_data[key].get(t, None) for t in out_df['time']]
                    for key in sorted(col_data)
                }
                out_df = out_df.hstack(pl.DataFrame(worker_data))

            return self.add_workflow_completion_percentage(out_df)

        write_df_to_csv(_process_rows_worker_storage_consumption(all_worker_storage, workers=self.dp.workers, percentage=False), self.csv_file_worker_storage_consumption)
        write_df_to_csv(_process_rows_worker_storage_consumption(all_worker_storage, workers=self.dp.workers, percentage=True), self.csv_file_worker_storage_consumption_percentage)

        def _generate_file_replica_activation_intervals():
            base_time = self.MIN_TIME
            rows = []
            for file in self.dp.files.values():
                flattened_transfers = file.get_flattened_transfers()
                if not flattened_transfers:
                    continue
                for transfer in flattened_transfers:
                    if transfer.time_stage_in is None or transfer.time_stage_out is None:
                        continue
                    dest = getattr(transfer, 'dest_worker_entry', None)
                    if dest is None:
                        continue
                    worker_str = f"{dest[0]}:{dest[1]}:{dest[2]}"
                    t_in = floor_decimal(float(transfer.time_stage_in - base_time), 2)
                    t_out = floor_decimal(float(transfer.time_stage_out - base_time), 2)
                    activation = floor_decimal(float(transfer.time_stage_out - transfer.time_stage_in), 2)
                    rows.append((file.filename, worker_str, t_in, t_out, activation))
            if not rows:
                df = pd.DataFrame(columns=['filename', 'replica_idx', 'source_worker', 'time_stage_in', 'time_stage_out', 'time_activation'])
                write_df_to_csv(df, self.csv_file_file_replica_activation_intervals, index=False)
                return
            rows.sort(key=lambda r: (r[0], r[2]))
            indexed_rows = []
            for idx, (fname, worker_str, t_in, t_out, activation) in enumerate(rows, start=1):
                indexed_rows.append({
                    'filename': fname,
                    'replica_idx': idx,
                    'source_worker': worker_str,
                    'time_stage_in': t_in,
                    'time_stage_out': t_out,
                    'time_activation': activation
                })                          
            df = pd.DataFrame(indexed_rows, columns=['filename', 'replica_idx', 'source_worker', 'time_stage_in', 'time_stage_out', 'time_activation'])
            write_df_to_csv(df, self.csv_file_file_replica_activation_intervals, index=False)

        _generate_file_replica_activation_intervals()

    def generate_task_metrics(self):
        filtered_tasks = [task for task in self.dp.tasks.values() if not task.is_library_task]
        if not filtered_tasks:
            return

        sorted_tasks = sorted(filtered_tasks, key=lambda t: (t.when_ready or float('inf')))
        base_time = self.MIN_TIME

        execution_time_rows = []
        response_time_rows = []
        retrieval_time_rows = []
        dependencies_rows = []
        dependents_rows = []
        finish_times = []

        output_to_task = {f: t.task_id for t in filtered_tasks for f in t.output_files}
        dependency_map = defaultdict(set)
        dependent_map = defaultdict(set)

        for task in filtered_tasks:
            task_id = task.task_id
            for f in task.input_files:
                parent_id = output_to_task.get(f)
                if parent_id and parent_id != task_id:
                    dependency_map[task_id].add(parent_id)
                    dependent_map[parent_id].add(task_id)

        for idx, task in enumerate(sorted_tasks, 1):
            tid = task.task_id
            try_id = task.task_try_id
            status = task.task_status

            ready = task.when_ready
            running = task.when_running
            start = task.time_worker_start
            end = task.time_worker_end
            fail = task.when_failure_happens
            retrieved = task.when_retrieved
            wait_retrieval = task.when_waiting_retrieval
            done = task.when_done

            def fd(t): return floor_decimal(t - base_time, 2) if t else None

            et = None
            ran = 0
            if status == 0 and end and start:
                et = max(fd(end) - fd(start), 0.01)
                ran = 1
            elif running and fail:
                et = max(fd(fail) - fd(running), 0.01)
            if et:
                execution_time_rows.append((idx, et, tid, try_id, ran))

            rt = None
            dispatched = 0
            if running and ready:
                rt = max(fd(running) - fd(ready), 0.01)
                dispatched = 1
            elif fail and ready:
                rt = max(fd(fail) - fd(ready), 0.01)
            if rt is not None:
                response_time_rows.append((idx, rt, tid, try_id, dispatched))

            if retrieved and wait_retrieval:
                rtt = max(fd(retrieved) - fd(wait_retrieval), 0.01)
                retrieval_time_rows.append((idx, rtt, tid, try_id))

            dependencies_rows.append((idx, len(dependency_map[tid])))
            dependents_rows.append((idx, len(dependent_map[tid])))

            finish = done or retrieved
            if finish:
                finish_times.append(fd(finish))

        def write_csv(data, cols, path):
            if data:
                df = pl.DataFrame(data, schema=cols, orient="row")
                df = downsample_df_polars(df, y_index=1, downsample_point_count=self.downsample_point_count)
            else:
                df = pl.DataFrame({col: [] for col in cols})
            write_df_to_csv(df, path, index=False)

        write_csv(execution_time_rows, ['Global Index', 'Execution Time', 'Task ID', 'Task Try ID', 'Ran to Completion'], self.csv_file_task_execution_time)
        write_csv(response_time_rows, ['Global Index', 'Response Time', 'Task ID', 'Task Try ID', 'Was Dispatched'], self.csv_file_task_response_time)
        write_csv(retrieval_time_rows, ['Global Index', 'Retrieval Time', 'Task ID', 'Task Try ID'], self.csv_file_task_retrieval_time)
        write_csv(dependencies_rows, ['Global Index', 'Dependency Count'], self.csv_file_task_dependencies)
        write_csv(dependents_rows, ['Global Index', 'Dependent Count'], self.csv_file_task_dependents)

        if finish_times:
            finish_times.sort()
            n = len(finish_times)
            percentiles = [(p, floor_decimal(finish_times[min(n - 1, max(0, math.ceil(p / 100 * n) - 1))], 2)) for p in range(1, 101)]
            write_csv(percentiles, ['Percentile', 'Completion Time'], self.csv_file_task_completion_percentiles)

    def generate_task_concurrency_data(self):
        filtered_tasks = [t for t in self.dp.tasks.values() if not t.is_library_task]
        if not filtered_tasks:
            return

        sorted_tasks = sorted(filtered_tasks, key=lambda t: (t.when_ready or float('inf')))
        base_time = self.MIN_TIME

        phase_titles = {
            'tasks_waiting': 'Waiting',
            'tasks_committing': 'Committing',
            'tasks_executing': 'Executing',
            'tasks_retrieving': 'Retrieving',
            'tasks_done': 'Done',
        }

        def _collect_phases(tasks):
            phases = defaultdict(list)

            def fd(t):
                return floor_decimal(t - base_time, 2) if t else None

            def add_phase(name, t0, t1=None):
                if t0:
                    phases[name].append((fd(t0), 1))
                if t0 and t1:
                    phases[name].append((fd(t1), -1))

            for task in tasks:
                ready = task.when_ready
                running = task.when_running
                start = task.time_worker_start
                end = task.time_worker_end
                fail = task.when_failure_happens
                wait_retrieval = task.when_waiting_retrieval
                done = task.when_done

                add_phase('tasks_waiting',    ready,                running or fail)
                add_phase('tasks_committing', running,              start or fail or wait_retrieval)
                add_phase('tasks_executing',  start,                end   or fail or wait_retrieval)
                add_phase('tasks_retrieving', end,                  wait_retrieval or fail)
                if done:
                    phases['tasks_done'].append((fd(done), 1))

            return phases

        def _build_concurrency_df(phases: dict[str, list[tuple[float, int]]]) -> pl.DataFrame | None:
            all_times = sorted({t for evs in phases.values() for (t, _) in evs if t is not None})
            if not all_times:
                return None

            df = pl.DataFrame({'time': all_times})
            first_time = all_times[0]

            for name in phase_titles:
                title = phase_titles[name]
                events = phases.get(name, [])

                if not events:
                    df = df.with_columns(pl.lit(0).alias(title))
                    continue

                df_phase = pl.DataFrame(
                    data=events,
                    schema={'time': pl.Float64, 'event': pl.Int32},
                    orient='row'
                ).drop_nulls('time').sort('time')

                df_phase = (
                    df_phase
                    .group_by('time')
                    .agg(pl.col('event').sum().alias('event'))
                    .with_columns(
                        # nonnegative cumulative sum without using clip APIs
                        pl.when(pl.col('event').cum_sum() < 0)
                        .then(pl.lit(0))
                        .otherwise(pl.col('event').cum_sum())
                        .alias(title)
                    )
                    .select(['time', title])
                )

                df = df.join(df_phase, on='time', how='left').sort('time')
                # emulate merge_asof(direction='backward'): carry last known value forward
                df = df.with_columns(pl.col(title).fill_null(strategy='forward'))

            # ensure columns exist and force first time point to 0
            df = df.with_columns([
                (pl.when(pl.col('time') == first_time).then(0).otherwise(pl.col(phase_titles[k])).alias(phase_titles[k])
                if phase_titles[k] in df.columns else pl.lit(0).alias(phase_titles[k]))
                for k in phase_titles
            ])
            df = self.add_workflow_completion_percentage(df)

            return df.fill_null(0)

        # normal
        task_phases = _collect_phases(sorted_tasks)
        time_df = _build_concurrency_df(task_phases)
        if time_df is not None and time_df.height > 0:
            pdf = time_df.to_pandas()
            pdf = downsample_df(pdf, y_index=1, downsample_point_count=self.downsample_point_count)
            write_df_to_csv(pdf, self.csv_file_task_concurrency, index=False)

        # recovery only
        recovery_tasks = [t for t in sorted_tasks if t.is_recovery_task]
        recovery_phases = _collect_phases(recovery_tasks)
        time_df = _build_concurrency_df(recovery_phases)
        if time_df is not None and time_df.height > 0:
            pdf = time_df.to_pandas()
            pdf = downsample_df(pdf, y_index=1, downsample_point_count=self.downsample_point_count)
            write_df_to_csv(pdf, self.csv_file_task_concurrency_recovery_only, index=False)
 
    def generate_task_execution_details_metrics(self):
        base_time = self.MIN_TIME
        rows = []

        for task in self.dp.tasks.values():
            if not hasattr(task, 'core_id') or not task.core_id:
                continue
            if not task.worker_entry:
                continue

            worker = self.dp.workers[task.worker_entry]
            worker_id = worker.id
            core_id = task.core_id[0]

            # Common task data
            task_data = {
                'task_id': task.task_id,
                'task_try_id': task.task_try_id,
                'worker_entry': f"{task.worker_entry[0]}:{task.worker_entry[1]}:{task.worker_entry[2]}",
                'worker_id': worker_id,
                'core_id': core_id,
                'is_recovery_task': task.is_recovery_task,
                'input_files': file_list_formatter(task.input_files) if task.input_files else '',
                'output_files': file_list_formatter(task.output_files) if task.output_files else '',
                'num_input_files': len(task.input_files) if task.input_files else 0,
                'num_output_files': len(task.output_files) if task.output_files else 0,
                'task_status': task.task_status,
                'category': getattr(task, 'category', ''),
                'when_ready': round(task.when_ready - base_time, 2) if task.when_ready else None,
                'when_running': round(task.when_running - base_time, 2) if task.when_running else None,
            }

            if task.task_status == 0:  # Successful task
                if not task.when_retrieved or getattr(task, 'is_library_task', False):
                    continue

                # Add successful task specific fields
                task_data.update({
                    'time_worker_start': round(task.time_worker_start - base_time, 2) if task.time_worker_start else None,
                    'time_worker_end': round(task.time_worker_end - base_time, 2) if task.time_worker_end else None,
                    'execution_time': round(task.time_worker_end - task.time_worker_start, 2) if task.time_worker_end and task.time_worker_start else None,
                    'when_waiting_retrieval': round(task.when_waiting_retrieval - base_time, 2) if task.when_waiting_retrieval else None,
                    'when_retrieved': round(task.when_retrieved - base_time, 2) if task.when_retrieved else None,
                    'when_done': round(task.when_done - base_time, 2) if task.when_done else None,
                    'record_type': 'successful_tasks',
                    'unsuccessful_checkbox_name': '',
                    'when_failure_happens': None,
                })
            else:  # Unsuccessful task
                task_data.update({
                    'time_worker_start': None,
                    'time_worker_end': None,
                    'when_waiting_retrieval': None,
                    'when_retrieved': None,
                    'when_failure_happens': round(task.when_failure_happens - base_time, 2) if task.when_failure_happens else None,
                    'execution_time': round(task.when_failure_happens - task.when_running, 2) if task.when_failure_happens and task.when_running else None,
                    'when_done': round(task.when_done - base_time, 2) if task.when_done else None,
                    'record_type': 'unsuccessful_tasks',
                    'unsuccessful_checkbox_name': TASK_STATUS_NAMES.get(task.task_status, 'unknown'),
                })

            rows.append(task_data)

        # Downsample task data with same logic as routes (separate successful and unsuccessful)
        def downsample_task_rows(task_rows, max_tasks=self.downsample_task_count):
            if len(task_rows) <= max_tasks:
                return task_rows
            task_rows_sorted = sorted(task_rows, key=lambda x: x.get('execution_time', 0) or 0, reverse=True)
            return task_rows_sorted[:max_tasks]
        
        successful_task_rows = [row for row in rows if row.get('record_type') == 'successful_tasks']
        unsuccessful_task_rows = [row for row in rows if row.get('record_type') == 'unsuccessful_tasks']
        other_rows = [row for row in rows if row.get('record_type') not in ['successful_tasks', 'unsuccessful_tasks']]

        # Downsample each type separately (same as routes)
        if self.downsampling:
            successful_task_rows = downsample_task_rows(successful_task_rows, max_tasks=self.downsample_task_count)
            unsuccessful_task_rows = downsample_task_rows(unsuccessful_task_rows, max_tasks=self.downsample_task_count)

        # Combine all rows back
        rows = other_rows + successful_task_rows + unsuccessful_task_rows

        # Add worker data
        for worker in self.dp.workers.values():
            if not getattr(worker, 'hash', None):
                continue

            # Ensure equal length lists for time_connected and time_disconnected
            time_disconnected = worker.time_disconnected[:]
            if len(time_disconnected) != len(worker.time_connected):
                time_disconnected.extend([self.MAX_TIME] * (len(worker.time_connected) - len(time_disconnected)))

            worker_data = {
                'task_id': pd.NA,
                'task_try_id': pd.NA,
                'worker_entry': f"{worker.ip}:{worker.port}:{worker.connect_id}",
                'worker_id': worker.id,
                'core_id': pd.NA,
                'is_recovery_task': pd.NA,
                'input_files': pd.NA,
                'output_files': pd.NA,
                'num_input_files': pd.NA,
                'num_output_files': pd.NA,
                'task_status': pd.NA,
                'category': pd.NA,
                'when_ready': pd.NA,
                'when_running': pd.NA,
                'time_worker_start': pd.NA,
                'time_worker_end': pd.NA,
                'execution_time': pd.NA,
                'when_waiting_retrieval': pd.NA,
                'when_retrieved': pd.NA,
                'when_failure_happens': pd.NA,
                'when_done': pd.NA,
                'record_type': 'worker',
                'unsuccessful_checkbox_name': pd.NA,
                'hash': worker.hash,
                'time_connected': [round(max(t - base_time, 0), 2) for t in worker.time_connected],
                'time_disconnected': [round(max(t - base_time, 0), 2) for t in time_disconnected],
                'cores': getattr(worker, 'cores', None),
                'memory_mb': getattr(worker, 'memory_mb', None),
                'disk_mb': getattr(worker, 'disk_mb', None),
                'gpus': getattr(worker, 'gpus', None)
            }
            rows.append(worker_data)
        
        # --- output ---

        if rows:
            df = pd.DataFrame(rows)
            
            # Define column order
            columns = [
                'record_type', 'task_id', 'task_try_id', 'worker_entry', 'worker_id', 'core_id',
                'is_recovery_task', 'input_files', 'output_files', 'num_input_files', 'num_output_files',
                'task_status', 'category', 'when_ready', 'when_running', 'time_worker_start',
                'time_worker_end', 'execution_time', 'when_waiting_retrieval', 'when_retrieved',
                'when_failure_happens', 'when_done', 'unsuccessful_checkbox_name', 'hash',
                'time_connected', 'time_disconnected', 'cores', 'memory_mb', 'disk_mb', 'gpus'
            ]
            
            # Reorder columns and fill missing ones with None
            for col in columns:
                if col not in df.columns: 
                    df[col] = None
            df = df[columns]
            
            write_df_to_csv(df, self.csv_file_task_execution_details, index=False)

    def generate_worker_metrics(self):
        base_time = self.MIN_TIME
        
        worker_lifetime_entries = []
        connect_events = []
        disconnect_events = []
        executing_task_events = defaultdict(list)
        waiting_retrieval_events = defaultdict(list)
        
        for worker in self.dp.workers.values():
            worker_key = worker.get_worker_key()
            worker_ip_port = ':'.join(worker_key.split(':')[:-1])  # Remove connect_id
            worker_id = worker.id
            worker_entry = (worker.ip, worker.port, worker.connect_id)
            
            for i, t_start in enumerate(worker.time_connected):
                t_end = (
                    worker.time_disconnected[i]
                    if i < len(worker.time_disconnected)
                    else self.MAX_TIME
                )
                t0 = floor_decimal(max(0, t_start - base_time), 2)
                t1 = floor_decimal(max(0, t_end - base_time), 2)
                duration = floor_decimal(max(0, t1 - t0), 2)
                worker_lifetime_entries.append((t0, duration, worker_id, worker_ip_port))
            
            for t in worker.time_connected:
                connect_events.append(floor_decimal(t - base_time, 2))
            for t in worker.time_disconnected:
                disconnect_events.append(floor_decimal(t - base_time, 2))
        
        for task in self.dp.tasks.values():
            if not task.worker_entry:
                continue
            
            worker_entry = task.worker_entry
            
            if task.time_worker_start and task.time_worker_end:
                start = floor_decimal(task.time_worker_start - base_time, 2)
                end = floor_decimal(task.time_worker_end - base_time, 2)
                if start < end:
                    executing_task_events[worker_entry].extend([(start, 1), (end, -1)])
            
            if task.when_waiting_retrieval and task.when_retrieved:
                start = floor_decimal(task.when_waiting_retrieval - base_time, 2)
                end = floor_decimal(task.when_retrieved - base_time, 2)
                if start < end:
                    waiting_retrieval_events[worker_entry].extend([(start, 1), (end, -1)])

        # Helper function for worker time series data
        def generate_worker_time_series_csv(events_dict, csv_file):
            if not events_dict:
                return

            column_data = {}
            time_set = set()

            for worker_entry, events in events_dict.items():
                w = self.dp.workers.get(worker_entry)
                if w:
                    t_connected = floor_decimal(w.time_connected[0] - base_time, 2)
                    t_disconnected = floor_decimal(w.time_disconnected[0] - base_time, 2)
                    boundary = []
                    if t_connected > 0:
                        boundary.append((t_connected, 0))
                    if t_disconnected > 0:
                        boundary.append((t_disconnected, 0))
                    events += boundary

                if not events:
                    continue

                df = pd.DataFrame(events, columns=['time', 'delta'])
                df = df.groupby('time', as_index=False)['delta'].sum()
                df['cumulative'] = df['delta'].cumsum().clip(lower=0)
                
                if df['cumulative'].isna().all():
                    continue

                downsampled_df = downsample_df(df[['time', 'cumulative']], y_col='cumulative', downsample_point_count=self.downsample_point_count)
                timeline = downsampled_df.values

                wid = f"{worker_entry[0]}:{worker_entry[1]}:{worker_entry[2]}"
                col_map = {t: v for t, v in timeline}
                column_data[wid] = col_map
                time_set.update(col_map.keys())

            if not column_data:
                return

            sorted_times = sorted(time_set)
            rows = []
            for t in sorted_times:
                row = {'time': floor_decimal(t, 2)}
                for c in sorted(column_data.keys()):
                    row[c] = column_data[c].get(t, float('nan'))
                rows.append(row)

            write_df_to_csv(pl.DataFrame(rows), csv_file, index=False)

        # Write CSV files
        
        # 1. Worker Lifetime
        if worker_lifetime_entries:
            worker_lifetime_entries.sort(key=lambda x: x[0])
            rows = [(worker_id, worker_ip_port, duration) for _, duration, worker_id, worker_ip_port in worker_lifetime_entries]
            write_df_to_csv(pl.DataFrame(rows, schema=['ID', 'Worker IP Port', 'LifeTime (s)'], orient="row"), self.csv_file_worker_lifetime, index=False)
        
        # 2. Worker Concurrency
        initial_active = sum(1 for t in connect_events if t <= 0)
        events = (
            [(t, 1) for t in connect_events if t > 0] +
            [(t, -1) for t in disconnect_events if t > 0]
        )
        
        if events or initial_active > 0:
            df = pd.DataFrame(events, columns=["time", "delta"])
            df = df.groupby("time", as_index=False)["delta"].sum().sort_values("time")
            
            df.loc[-1] = [0.0, 0]
            df = df.sort_index().reset_index(drop=True)
            
            df["active"] = df["delta"].cumsum() + initial_active
            
            max_time = floor_decimal(self.MAX_TIME - base_time, 2)
            if df.iloc[-1]["time"] < max_time:
                last_active = df.iloc[-1]["active"]
                new_row = pd.DataFrame({"time": [max_time], "delta": [0], "active": [last_active]})
                df = pd.concat([df, new_row], ignore_index=True)

            export_df = df[['time', 'active']].rename(columns={'time': 'time', 'active': 'Active Workers (count)'})
            export_df = downsample_df(export_df, y_col='Active Workers (count)', downsample_point_count=self.downsample_point_count)
            write_df_to_csv(export_df, self.csv_file_worker_concurrency, index=False)
        
        # 3. Worker Executing Tasks
        generate_worker_time_series_csv(executing_task_events, self.csv_file_worker_executing_tasks)
        
        # 4. Worker Waiting Retrieval Tasks
        generate_worker_time_series_csv(waiting_retrieval_events, self.csv_file_worker_waiting_retrieval_tasks)

    def generate_subgraphs_and_graph_metrics(self):
        # Step 1: Find unique tasks (exclude library and recovery tasks)
        unique_tasks = {}
        task_failure_counts = defaultdict(int)
        
        for (tid, try_id), task in self.dp.tasks.items():
            if getattr(task, 'is_library_task', False) or getattr(task, 'is_recovery_task', False):
                continue
            
            if task.task_status is None:
                raise ValueError(f"task {tid}:{try_id} has None task_status, this should not happen")
            
            if task.task_status != 0:
                task_failure_counts[tid] += 1

            if tid not in unique_tasks:
                # first time seeing this task_id, add it
                unique_tasks[tid] = task
            else:
                # already have this task_id, only replace if current is failed and new is successful
                if unique_tasks[tid].task_status != 0 and task.task_status == 0:
                    unique_tasks[tid] = task
                # otherwise skip this task entry

        if not unique_tasks:
            return

        # Step 2: Generate subgraphs using unique tasks
        tasks_keys = set((task.task_id, task.task_try_id) for task in unique_tasks.values())
        parent = {}

        def _find(x):
            parent.setdefault(x, x)
            if parent[x] != x:
                parent[x] = _find(parent[x])  # path compression
            return parent[x]

        def _union(x, y):
            root_x = _find(x)
            root_y = _find(y)
            if root_x != root_y:
                parent[root_x] = root_y

        dependency_count = 0
        files_with_dependencies = 0
        
        for file in self.dp.files.values():
            if not file.producers:
                continue
            tasks_involved = (set(file.producers) | set(file.consumers)) & tasks_keys
            if len(tasks_involved) <= 1:
                continue
            
            files_with_dependencies += 1
            dependency_count += len(tasks_involved) - 1
            
            tasks_involved = list(tasks_involved)
            first_task = tasks_involved[0]
            for other_task in tasks_involved[1:]:
                _union(first_task, other_task)

        subgraphs = defaultdict(set)
        for task_key in tasks_keys:
            root = _find(task_key)
            subgraphs[root].add(task_key)

        sorted_subgraphs = sorted(subgraphs.values(), key=len, reverse=True)
        self.subgraphs = {i: subgraph for i, subgraph in enumerate(sorted_subgraphs, 1)}
        if len(self.subgraphs) == 0:
            return

        # Step 3: Generate CSV with task dependencies
        task_to_subgraph = {}
        for subgraph_id, task_entries in self.subgraphs.items():
            for (tid, try_id) in task_entries:
                if tid in unique_tasks:
                    task_to_subgraph[tid] = subgraph_id

        recovery_count_map = {}
        for task in unique_tasks.values():
            task_id = task.task_id
            recovery_task_id_set = set()
            for file_name in task.output_files:
                file_obj = self.dp.files[file_name]
                for (producer_tid, producer_try_id) in file_obj.producers:
                    producer_task = self.dp.tasks[(producer_tid, producer_try_id)]
                    if producer_task.is_recovery_task:
                        recovery_task_id_set.add(producer_tid)
            recovery_count_map[task_id] = len(recovery_task_id_set)

        rows = []
        for task in unique_tasks.values():
            task_id = task.task_id
            subgraph_id = task_to_subgraph.get(task_id, 0)
            failure_count = task_failure_counts.get(task_id, 0)
            recovery_count = recovery_count_map.get(task_id, 0)
            
            input_files_with_timing = []
            for file_name in getattr(task, 'input_files', []):
                # Only include files that have producers (are in dependency graph)
                if file_name in self.dp.files and self.dp.files[file_name].producers:
                    file_obj = self.dp.files[file_name]
                    if task.when_running and file_obj.created_time:
                        waiting_time = max(0, task.when_running - file_obj.created_time)
                        input_files_with_timing.append(f"{file_name}:{waiting_time:.2f}")
                    else:
                        input_files_with_timing.append(f"{file_name}:0.00")

            output_files_with_timing = []
            for file_name in getattr(task, 'output_files', []):
                file = self.dp.files[file_name]
                # skip files without any producer tasks
                if len(file.producers) == 0:
                    continue
                creation_time = 0.0
                
                # skip tasks that were not committed to a worker
                if not task.worker_entry:
                    continue
                if not task.time_worker_start:
                    continue

                for transfer in file.get_flattened_transfers():
                    if transfer.dest_worker_entry != task.worker_entry:
                        continue
                    if not transfer.time_stage_in:
                        continue
                    if transfer.time_stage_in < task.time_worker_start:
                        continue
                    creation_time = max(0, transfer.time_stage_in - task.time_worker_start)
                    break

                if creation_time == 0.0 and file.created_time and task.time_worker_start:
                    creation_time = max(0, file.created_time - task.time_worker_start)
                
                output_files_with_timing.append(f"{file_name}:{creation_time:.2f}")
            
            input_files_str = '|'.join(input_files_with_timing) if input_files_with_timing else ''
            output_files_str = '|'.join(output_files_with_timing) if output_files_with_timing else ''
            
            # Calculate task execution time
            execution_time = np.nan
            if task.task_status == 0 and task.time_worker_start and task.time_worker_end:
                execution_time = max(0, task.time_worker_end - task.time_worker_start)
            
            rows.append([
                subgraph_id,
                task_id,
                execution_time,
                failure_count,
                recovery_count,
                input_files_str,
                output_files_str
            ])

        if len(rows) == 0:
            return

        df = pd.DataFrame(rows, columns=[
            'subgraph_id', 'task_id', 'task_execution_time', 'failure_count', 'recovery_count', 'input_files', 'output_files'
        ])
        df = df.sort_values(['subgraph_id', 'task_id'])
        write_df_to_csv(df, self.csv_file_task_subgraphs, index=False)

    def generate_metadata(self):
        metadata = {}
        
        # Worker statistics
        metadata['total_workers'] = len(self.dp.workers)
        
        # Task statistics
        # Filter non-library tasks for main statistics
        non_library_tasks = [task for task in self.dp.tasks.values() if not getattr(task, 'is_library_task', False)]
        library_tasks = [task for task in self.dp.tasks.values() if getattr(task, 'is_library_task', False)]
        
        metadata['total_tasks'] = len(non_library_tasks)
        metadata['total_library_tasks'] = len(library_tasks)
        metadata['total_all_tasks'] = len(self.dp.tasks)
        
        # Task status statistics
        task_status_counts = {}
        successful_tasks = 0
        unsuccessful_tasks = 0
        recovery_tasks = 0
        dispatched_tasks = 0
        undispatched_tasks = 0
        failed_tasks = 0
        
        for task in non_library_tasks:
            task_status = getattr(task, 'task_status', None)
            is_recovery = getattr(task, 'is_recovery_task', False)
            when_running = getattr(task, 'when_running', None)
            
            # Count by task status
            if task_status is not None:
                task_status_counts[task_status] = task_status_counts.get(task_status, 0) + 1
                
                # Successful vs unsuccessful
                if task_status == 0:
                    successful_tasks += 1
                else:
                    unsuccessful_tasks += 1
                    failed_tasks += 1
                    
                # Dispatched vs undispatched
                if task_status == (42 << 3):  # undispatched
                    undispatched_tasks += 1
                elif when_running is not None:
                    dispatched_tasks += 1
                else:
                    undispatched_tasks += 1
            
            # Recovery tasks
            if is_recovery:
                recovery_tasks += 1
        
        metadata['task_status_counts'] = task_status_counts
        metadata['successful_tasks'] = successful_tasks
        metadata['unsuccessful_tasks'] = unsuccessful_tasks
        metadata['recovery_tasks'] = recovery_tasks
        metadata['dispatched_tasks'] = dispatched_tasks
        metadata['undispatched_tasks'] = undispatched_tasks
        metadata['failed_tasks'] = failed_tasks
        
        # Recovery task breakdown
        recovery_successful = 0
        recovery_unsuccessful = 0
        for task in non_library_tasks:
            if getattr(task, 'is_recovery_task', False):
                task_status = getattr(task, 'task_status', None)
                if task_status == 0:
                    recovery_successful += 1
                else:
                    recovery_unsuccessful += 1
        
        metadata['recovery_successful'] = recovery_successful
        metadata['recovery_unsuccessful'] = recovery_unsuccessful
        
        metadata['total_files'] = len(self.dp.files)
        
        metadata['manager_start_time'] = self.dp.manager.time_start
        metadata['manager_end_time'] = self.dp.manager.time_end
        metadata['manager_duration'] = (self.dp.manager.time_end - self.dp.manager.time_start) if (self.dp.manager.time_start and self.dp.manager.time_end) else None
        
        rows = [{"key": k, "value": json.dumps(v, ensure_ascii=False)} for k, v in metadata.items()]
        pl.DataFrame(rows).write_csv(self.csv_file_metadata)