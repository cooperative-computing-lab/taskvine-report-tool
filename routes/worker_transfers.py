from flask import Blueprint, jsonify, make_response
import pandas as pd
from collections import defaultdict
import math
from io import StringIO

from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_time_formatter,
    d3_int_formatter,
    downsample_points_array
)

worker_transfers_bp = Blueprint('worker_transfers', __name__, url_prefix='/api')

def get_worker_transfer_raw_points(role):
    assert role in ['source', 'destination']

    base_time = runtime_state.MIN_TIME
    transfers_by_worker = defaultdict(list)

    for file in runtime_state.files.values():
        for transfer in file.transfers:
            worker = getattr(transfer, role)
            if not isinstance(worker, tuple):
                continue
            transfers_by_worker[worker].append((transfer.time_start_stage_in - base_time, 1))
            if transfer.time_stage_in:
                transfers_by_worker[worker].append((transfer.time_stage_in - base_time, -1))
            elif transfer.time_stage_out:
                transfers_by_worker[worker].append((transfer.time_stage_out - base_time, -1))

    worker_keys = []
    raw_points_array = []

    for worker, events in transfers_by_worker.items():
        df = pd.DataFrame(events, columns=['time', 'event']).sort_values('time')
        df['time'] = df['time'].round(2)
        df['cumulative'] = df['event'].cumsum()
        df = df.drop_duplicates('time', keep='last')
        points = df[['time', 'cumulative']].values.tolist()

        for t0, t1 in zip(runtime_state.workers[worker].time_connected,
                          runtime_state.workers[worker].time_disconnected):
            points.insert(0, [round(t0 - base_time, 2), 0])
            points.append([round(t1 - base_time, 2), 0])

        if not points or any(len(p) != 2 or math.isnan(p[0]) or math.isnan(p[1]) for p in points):
            continue

        worker_keys.append(worker)
        raw_points_array.append(points)

    return worker_keys, raw_points_array


@worker_transfers_bp.route('/worker-incoming-transfers')
@check_and_reload_data()
def get_worker_incoming_transfers():
    try:
        worker_keys, raw_points_array = get_worker_transfer_raw_points('destination')
        if not raw_points_array:
            return jsonify({'error': 'No incoming transfer data'}), 404

        downsampled_array = downsample_points_array(raw_points_array, SAMPLING_POINTS)
        transfers = {}
        max_y = 0
        for worker, points in zip(worker_keys, downsampled_array):
            wid = f"{worker[0]}:{worker[1]}"
            transfers[wid] = points
            max_y = max(max_y, max(p[1] for p in points))

        return jsonify({
            'transfers': transfers,
            'x_domain': [0, runtime_state.MAX_TIME - runtime_state.MIN_TIME],
            'y_domain': [0, int(max_y)],
            'x_tick_values': compute_linear_tick_values([0, runtime_state.MAX_TIME - runtime_state.MIN_TIME]),
            'y_tick_values': compute_linear_tick_values([0, int(max_y)]),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        })

    except Exception as e:
        runtime_state.log_error(f"Error in get_worker_incoming_transfers: {e}")
        return jsonify({'error': str(e)}), 500


@worker_transfers_bp.route('/worker-outgoing-transfers')
@check_and_reload_data()
def get_worker_outgoing_transfers():
    try:
        worker_keys, raw_points_array = get_worker_transfer_raw_points('source')
        if not raw_points_array:
            return jsonify({'error': 'No outgoing transfer data'}), 404

        downsampled_array = downsample_points_array(raw_points_array, SAMPLING_POINTS)
        transfers = {}
        max_y = 0
        for worker, points in zip(worker_keys, downsampled_array):
            wid = f"{worker[0]}:{worker[1]}"
            transfers[wid] = points
            max_y = max(max_y, max(p[1] for p in points))

        return jsonify({
            'transfers': transfers,
            'x_domain': [0, runtime_state.MAX_TIME - runtime_state.MIN_TIME],
            'y_domain': [0, int(max_y)],
            'x_tick_values': compute_linear_tick_values([0, runtime_state.MAX_TIME - runtime_state.MIN_TIME]),
            'y_tick_values': compute_linear_tick_values([0, int(max_y)]),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        })

    except Exception as e:
        runtime_state.log_error(f"Error in get_worker_outgoing_transfers: {e}")
        return jsonify({'error': str(e)}), 500


@worker_transfers_bp.route('/worker-incoming-transfers/export-csv')
@check_and_reload_data()
def export_worker_incoming_transfers_csv():
    try:
        worker_keys, raw_points_array = get_worker_transfer_raw_points('destination')
        if not raw_points_array:
            return jsonify({'error': 'No incoming transfer data'}), 404

        merged_df = None
        for worker, points in zip(worker_keys, raw_points_array):
            wid = f"{worker[0]}:{worker[1]}"
            df = pd.DataFrame(points, columns=["time", wid])
            if merged_df is None:
                merged_df = df
            else:
                merged_df = pd.merge(merged_df, df, on="time", how="outer")

        merged_df = merged_df.sort_values("time").fillna(0).round(2)
        merged_df.columns = ["time (s)"] + [f"{col}" for col in merged_df.columns[1:]]

        buffer = StringIO()
        merged_df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=worker_incoming_transfers.csv'
        response.headers['Content-Type'] = 'text/csv'
        return response
    except Exception as e:
        runtime_state.log_error(f"Error in export_worker_incoming_transfers_csv: {e}")
        return jsonify({'error': str(e)}), 500
    
@worker_transfers_bp.route('/worker-outgoing-transfers/export-csv')
@check_and_reload_data()
def export_worker_outgoing_transfers_csv():
    try:
        worker_keys, raw_points_array = get_worker_transfer_raw_points('source')
        if not raw_points_array:
            return jsonify({'error': 'No outgoing transfer data'}), 404

        merged_df = None
        for worker, points in zip(worker_keys, raw_points_array):
            wid = f"{worker[0]}:{worker[1]}"
            df = pd.DataFrame(points, columns=["time", wid])
            if merged_df is None:
                merged_df = df
            else:
                merged_df = pd.merge(merged_df, df, on="time", how="outer")

        merged_df = merged_df.sort_values("time").fillna(0).round(2)
        merged_df.columns = ["time (s)"] + [f"{col}" for col in merged_df.columns[1:]]

        buffer = StringIO()
        merged_df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=worker_outgoing_transfers.csv'
        response.headers['Content-Type'] = 'text/csv'
        return response
    except Exception as e:
        runtime_state.log_error(f"Error in export_worker_outgoing_transfers_csv: {e}")
        return jsonify({'error': str(e)}), 500