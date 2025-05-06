from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from src.utils import get_unit_and_scale_by_max_file_size_mb

import pandas as pd
import numpy as np
import random
import traceback
from flask import Blueprint, jsonify, request


storage_consumption_bp = Blueprint(
    'storage_consumption', __name__, url_prefix='/api')

def downsample_storage_data(points):
    # downsample storage consumption data points while keeping the global peak and randomly sampling other points
    if len(points) <= SAMPLING_POINTS:
        return points

    global_peak_idx = max(range(len(points)), key=lambda i: points[i][1])
    global_peak = points[global_peak_idx]

    keep_indices = {0, len(points) - 1, global_peak_idx}

    remaining_points = SAMPLING_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, points[-1]]

    available_indices = list(set(range(len(points))) - keep_indices)
    sampled_indices = random.sample(available_indices, min(
        remaining_points, len(available_indices)))
    keep_indices.update(sampled_indices)

    result = [points[i] for i in sorted(keep_indices)]
    return result


@storage_consumption_bp.route('/storage-consumption')
@check_and_reload_data()
def get_storage_consumption():
    try:
        show_percentage = request.args.get(
            'show_percentage', 'false').lower() == 'true'
        show_pbb_workers = request.args.get(
            'show_pbb_workers', 'true').lower() == 'true'
        data = {}

        files = runtime_state.files

        # construct the succeeded file transfers
        data['worker_storage_consumption'] = {}
        data['worker_resources'] = {}

        all_worker_storage = {}

        for file in files.values():
            for transfer in file.transfers:
                # skip if this is not a transfer to a worker
                if not isinstance(transfer.destination, tuple):
                    continue
                # skip if the transfer was not successful
                if transfer.time_stage_in is None:
                    continue

                destination = transfer.destination

                # skip if this is a PBB worker
                if destination in runtime_state.workers:
                    if runtime_state.workers[destination].is_pbb:
                        if not show_pbb_workers:
                            continue

                # add the transfer to the worker
                if destination not in all_worker_storage:
                    all_worker_storage[destination] = []

                # time_in = float(transfer.time_stage_in - runtime_state.MIN_TIME)
                time_in = float(transfer.time_start_stage_in -
                                runtime_state.MIN_TIME)
                time_out = float(transfer.time_stage_out -
                                 runtime_state.MIN_TIME)

                all_worker_storage[destination].append(
                    (time_in, max(0, file.size_mb)))
                all_worker_storage[destination].append(
                    (time_out, -max(0, file.size_mb)))

        for destination in list(all_worker_storage.keys()):
            data['worker_storage_consumption'][destination] = all_worker_storage[destination]

        max_storage_consumption = 0
        for destination in list(data['worker_storage_consumption'].keys()):
            if not data['worker_storage_consumption'][destination]:
                del data['worker_storage_consumption'][destination]
                continue

            # convert to a pandas dataframe
            df = pd.DataFrame(data['worker_storage_consumption']
                              [destination], columns=['time', 'size'])
            # sort the dataframe by time
            df = df.sort_values(by=['time'])
            # accumulate the size
            df['storage_consumption'] = df['size'].cumsum()
            df['storage_consumption'] = df['storage_consumption'].clip(
                lower=0)  # Ensure no negative values

            if show_percentage:
                # Use worker's total disk space from worker.disk_mb
                worker_disk_mb = runtime_state.workers[destination].disk_mb
                if worker_disk_mb > 0:  # Avoid division by zero
                    df['storage_consumption'] = (
                        df['storage_consumption'] / worker_disk_mb) * 100

            # group by time and keep the maximum storage consumption for each time
            df = df.groupby('time')['storage_consumption'].max().reset_index()

            # Skip if no valid data
            if df.empty or df['storage_consumption'].isna().all():
                del data['worker_storage_consumption'][destination]
                continue

            # update the max storage consumption
            curr_max = df['storage_consumption'].max()
            if not np.isnan(curr_max):
                max_storage_consumption = max(
                    max_storage_consumption, curr_max)

            # Convert to list of points and downsample
            points = df[['time', 'storage_consumption']].values.tolist()
            points = downsample_storage_data(points)
            data['worker_storage_consumption'][destination] = points

            # add the initial and final points with 0 consumption
            worker = runtime_state.workers[destination]
            if worker.time_connected and worker.time_disconnected:
                for time_connected, time_disconnected in zip(worker.time_connected, worker.time_disconnected):
                    time_start = float(time_connected - runtime_state.MIN_TIME)
                    time_end = float(time_disconnected -
                                     runtime_state.MIN_TIME)
                    if not np.isnan(time_start):
                        data['worker_storage_consumption'][destination].insert(
                            0, [time_start, 0.0])
                    if not np.isnan(time_end):
                        data['worker_storage_consumption'][destination].append([
                                                                               time_end, 0.0])

        # Skip if no valid data for any worker
        if not data['worker_storage_consumption']:
            return jsonify({'error': 'No valid storage consumption data available'}), 404

        # convert the key to a string
        data['worker_storage_consumption'] = {
            f"{k[0]}:{k[1]}": v for k, v in data['worker_storage_consumption'].items()}

        if show_percentage:
            data['file_size_unit'] = '%'
            max_storage_consumption = 100
        else:
            data['file_size_unit'], scale = get_unit_and_scale_by_max_file_size_mb(
                max_storage_consumption)
            # also update the source data
            if scale != 1:
                for destination in data['worker_storage_consumption']:
                    points = data['worker_storage_consumption'][destination]
                    data['worker_storage_consumption'][destination] = [
                        [p[0], p[1] * scale] for p in points]
                max_storage_consumption *= scale

        # Add worker resource information
        for worker_entry, worker in runtime_state.workers.items():
            worker_id = f"{worker_entry[0]}:{worker_entry[1]}"
            if worker_id in data['worker_storage_consumption']:
                data['worker_resources'][worker_id] = {
                    'cores': worker.cores,
                    'memory_mb': worker.memory_mb,
                    'disk_mb': worker.disk_mb,
                    'gpus': worker.gpus,
                    'is_pbb': getattr(worker, 'is_pbb', False)
                }

        data['show_pbb_workers'] = show_pbb_workers

        # plotting parameters
        data['xMin'] = 0
        data['xMax'] = float(runtime_state.MAX_TIME - runtime_state.MIN_TIME)
        data['yMin'] = 0
        # Ensure positive yMax and at least 1.0
        data['yMax'] = max(1.0, max_storage_consumption)

        # Ensure all tick values are valid numbers
        x_range = data['xMax'] - data['xMin']
        data['xTickValues'] = [
            float(data['xMin']),
            float(data['xMin'] + x_range * 0.25),
            float(data['xMin'] + x_range * 0.5),
            float(data['xMin'] + x_range * 0.75),
            float(data['xMax'])
        ]

        y_range = data['yMax'] - data['yMin']
        data['yTickValues'] = [
            float(data['yMin']),
            float(data['yMin'] + y_range * 0.25),
            float(data['yMin'] + y_range * 0.5),
            float(data['yMin'] + y_range * 0.75),
            float(data['yMax'])
        ]

        data['tickFontSize'] = int(runtime_state.tick_size)

        return jsonify(data)

    except Exception as e:
        print(f"Error in get_storage_consumption: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
