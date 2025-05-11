from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from flask import Blueprint, jsonify
import pandas as pd
from collections import defaultdict
from .utils import compute_tick_values

worker_transfers_bp = Blueprint('worker_transfers', __name__, url_prefix='/api')

def downsample_worker_transfers(points):
    # downsample while keeping first, last, and peak points
    if len(points) <= SAMPLING_POINTS:
        return points

    # find global peak (maximum transfers)
    global_peak_idx = max(range(len(points)), key=lambda i: points[i][1])
    global_peak = points[global_peak_idx]

    # keep key points
    keep_indices = {0, len(points) - 1, global_peak_idx}

    # calculate remaining points to sample
    remaining_points = SAMPLING_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, points[-1]]

    # sort indices to find gaps
    sorted_keep_indices = sorted(keep_indices)

    # calculate points per gap
    points_per_gap = remaining_points // (len(sorted_keep_indices) - 1)
    extra_points = remaining_points % (len(sorted_keep_indices) - 1)

    # for each gap, sample points randomly
    for i in range(len(sorted_keep_indices) - 1):
        start_idx = sorted_keep_indices[i]
        end_idx = sorted_keep_indices[i + 1]
        gap_size = end_idx - start_idx - 1

        if gap_size <= 0:
            continue

        # calculate points for this gap
        current_gap_points = points_per_gap
        if extra_points > 0:
            current_gap_points += 1
            extra_points -= 1

        if current_gap_points > 0:
            # randomly sample from gap
            available_indices = list(range(start_idx + 1, end_idx))
            sampled_indices = random.sample(available_indices, min(
                current_gap_points, len(available_indices)))
            keep_indices.update(sampled_indices)

    # return sorted points
    result = [points[i] for i in sorted(keep_indices)]
    return result

@worker_transfers_bp.route('/worker-incoming-transfers')
@check_and_reload_data()
def get_worker_incoming_transfers():
    try:
        data = {}

        # construct the file transfers
        data['transfers'] = defaultdict(list)     # for destinations
        for file in runtime_state.files.values():
            for transfer in file.transfers:
                destination = transfer.destination

                # only consider file transfers to workers
                if not isinstance(destination, tuple):
                    continue

                data['transfers'][destination].append(
                    (round(transfer.time_start_stage_in - runtime_state.MIN_TIME, 2), 1))
                if transfer.time_stage_in:
                    data['transfers'][destination].append(
                        (round(transfer.time_stage_in - runtime_state.MIN_TIME, 2), -1))
                elif transfer.time_stage_out:
                    data['transfers'][destination].append(
                        (round(transfer.time_stage_out - runtime_state.MIN_TIME, 2), -1))

        max_transfers = 0
        for worker in data['transfers']:
            df = pd.DataFrame(data['transfers'][worker],
                            columns=['time', 'event'])
            df = df.sort_values(by=['time'])
            df['cumulative_transfers'] = df['event'].cumsum()
            # if two rows have the same time, keep the one with the largest event
            df = df.drop_duplicates(subset=['time'], keep='last')

            # Convert to list of points and downsample
            points = df[['time', 'cumulative_transfers']].values.tolist()
            points = downsample_worker_transfers(points)
            data['transfers'][worker] = points

            # append the initial point at time_connected with 0
            for time_connected, time_disconnected in zip(runtime_state.workers[worker].time_connected, runtime_state.workers[worker].time_disconnected):
                data['transfers'][worker].insert(
                    0, [time_connected - runtime_state.MIN_TIME, 0])
                data['transfers'][worker].append(
                    [time_disconnected - runtime_state.MIN_TIME, 0])
            max_transfers = max(max_transfers, max(
                point[1] for point in points))

        # convert keys to string-formatted keys
        data['transfers'] = {f"{k[0]}:{k[1]}": v for k,
                            v in data['transfers'].items()}

        # plotting parameters
        data['x_domain'] = [0, runtime_state.MAX_TIME - runtime_state.MIN_TIME]
        data['y_domain'] = [0, int(max_transfers)]
        data['x_tick_values'] = compute_tick_values(data['x_domain'])
        data['y_tick_values'] = compute_tick_values(data['y_domain'])

        return jsonify(data)

    except Exception as e:
        print(f"Error in get_worker_incoming_transfers: {str(e)}")
        return jsonify({'error': str(e)}), 500

@worker_transfers_bp.route('/worker-outgoing-transfers')
@check_and_reload_data()
def get_worker_outgoing_transfers():
    try:
        data = {}

        # construct the file transfers
        data['transfers'] = defaultdict(list)     # for sources
        for file in runtime_state.files.values():
            for transfer in file.transfers:
                source = transfer.source

                # only consider file transfers from workers
                if not isinstance(source, tuple):
                    continue

                data['transfers'][source].append(
                    (round(transfer.time_start_stage_in - runtime_state.MIN_TIME, 2), 1))
                if transfer.time_stage_in:
                    data['transfers'][source].append(
                        (round(transfer.time_stage_in - runtime_state.MIN_TIME, 2), -1))
                elif transfer.time_stage_out:
                    data['transfers'][source].append(
                        (round(transfer.time_stage_out - runtime_state.MIN_TIME, 2), -1))

        max_transfers = 0
        for worker in data['transfers']:
            df = pd.DataFrame(data['transfers'][worker],
                            columns=['time', 'event'])
            df = df.sort_values(by=['time'])
            df['cumulative_transfers'] = df['event'].cumsum()
            # if two rows have the same time, keep the one with the largest event
            df = df.drop_duplicates(subset=['time'], keep='last')

            # Convert to list of points and downsample
            points = df[['time', 'cumulative_transfers']].values.tolist()
            points = downsample_worker_transfers(points)
            data['transfers'][worker] = points

            # append the initial point at time_connected with 0
            for time_connected, time_disconnected in zip(runtime_state.workers[worker].time_connected, runtime_state.workers[worker].time_disconnected):
                data['transfers'][worker].insert(
                    0, [time_connected - runtime_state.MIN_TIME, 0])
                data['transfers'][worker].append(
                    [time_disconnected - runtime_state.MIN_TIME, 0])
            max_transfers = max(max_transfers, max(
                point[1] for point in points))

        # convert keys to string-formatted keys
        data['transfers'] = {f"{k[0]}:{k[1]}": v for k,
                            v in data['transfers'].items()}

        # plotting parameters
        data['x_domain'] = [0, runtime_state.MAX_TIME - runtime_state.MIN_TIME]
        data['y_domain'] = [0, int(max_transfers)]
        data['x_tick_values'] = compute_tick_values(data['x_domain'])
        data['y_tick_values'] = compute_tick_values(data['y_domain'])

        return jsonify(data)

    except Exception as e:
        print(f"Error in get_worker_outgoing_transfers: {str(e)}")
        return jsonify({'error': str(e)}), 500 