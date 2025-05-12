from flask import Blueprint, jsonify
import pandas as pd
from collections import defaultdict
import math

from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_tick_values,
    d3_time_formatter,
    d3_int_formatter,
    downsample_points_array
)

worker_transfers_bp = Blueprint('worker_transfers', __name__, url_prefix='/api')

def extract_worker_transfer_points(role):
    assert role in ['source', 'destination']

    base_time = runtime_state.MIN_TIME
    transfers_by_worker = defaultdict(list)

    for file in runtime_state.files.values():
        for transfer in file.transfers:
            worker = getattr(transfer, role)
            if not isinstance(worker, tuple):
                continue

            transfers_by_worker[worker].append((round(transfer.time_start_stage_in - base_time, 2), 1))
            if transfer.time_stage_in:
                transfers_by_worker[worker].append((round(transfer.time_stage_in - base_time, 2), -1))
            elif transfer.time_stage_out:
                transfers_by_worker[worker].append((round(transfer.time_stage_out - base_time, 2), -1))

    worker_keys = []
    raw_points_array = []

    for worker, events in transfers_by_worker.items():
        df = pd.DataFrame(events, columns=['time', 'event']).sort_values('time')
        df['cumulative'] = df['event'].cumsum()
        df = df.drop_duplicates('time', keep='last')
        points = df[['time', 'cumulative']].values.tolist()

        for t0, t1 in zip(runtime_state.workers[worker].time_connected,
                          runtime_state.workers[worker].time_disconnected):
            points.insert(0, [t0 - base_time, 0])
            points.append([t1 - base_time, 0])

        if not points or any(len(p) != 2 or math.isnan(p[0]) or math.isnan(p[1]) for p in points):
            continue

        worker_keys.append(worker)
        raw_points_array.append(points)

    downsampled_array = downsample_points_array(raw_points_array, SAMPLING_POINTS)

    transfers = {}
    max_y = 0
    for worker, points in zip(worker_keys, downsampled_array):
        transfers[f"{worker[0]}:{worker[1]}"] = points
        max_y = max(max_y, max(p[1] for p in points))

    data = {
        'transfers': transfers,
        'x_domain': [0, runtime_state.MAX_TIME - base_time],
        'y_domain': [0, int(max_y)],
        'x_tick_values': compute_tick_values([0, runtime_state.MAX_TIME - base_time]),
        'y_tick_values': compute_tick_values([0, int(max_y)]),
        'x_tick_formatter': d3_time_formatter(),
        'y_tick_formatter': d3_int_formatter(),
    }

    return data

@worker_transfers_bp.route('/worker-incoming-transfers')
@check_and_reload_data()
def get_worker_incoming_transfers():
    try:
        return jsonify(extract_worker_transfer_points('destination'))
    except Exception as e:
        print(f"Error in get_worker_incoming_transfers: {str(e)}")
        return jsonify({'error': str(e)}), 500

@worker_transfers_bp.route('/worker-outgoing-transfers')
@check_and_reload_data()
def get_worker_outgoing_transfers():
    try:
        return jsonify(extract_worker_transfer_points('source'))
    except Exception as e:
        print(f"Error in get_worker_outgoing_transfers: {str(e)}")
        return jsonify({'error': str(e)}), 500
