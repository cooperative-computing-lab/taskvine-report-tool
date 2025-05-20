from flask import Blueprint, jsonify, make_response, Response
import pandas as pd
from collections import defaultdict
from io import StringIO

from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_time_formatter,
    d3_int_formatter,
    downsample_points_array,
    floor_decimal,
    compress_time_based_critical_points
)

worker_transfers_bp = Blueprint('worker_transfers', __name__, url_prefix='/api')

def get_worker_transfer_raw_points(role):
    assert role in ['outgoing', 'incoming']
    if role == 'outgoing':
        role = 'source'
    else:
        role = 'destination'

    base_time = runtime_state.MIN_TIME
    transfers_by_worker = defaultdict(list)

    for file in runtime_state.files.values():
        for transfer in file.transfers:
            worker = getattr(transfer, role)
            if not isinstance(worker, tuple):
                continue
            t0 = floor_decimal(transfer.time_start_stage_in - base_time, 2)
            transfers_by_worker[worker].append((t0, 1))

            if transfer.time_stage_in:
                t1 = floor_decimal(transfer.time_stage_in - base_time, 2)
            elif transfer.time_stage_out:
                t1 = floor_decimal(transfer.time_stage_out - base_time, 2)
            else:
                continue
            transfers_by_worker[worker].append((t1, -1))

    raw_points_array = []
    worker_keys = [] 

    for worker, events in transfers_by_worker.items():
        if not events:
            continue

        w = runtime_state.workers.get(worker)
        boundary_events = []
        if w:
            boundary_times = [floor_decimal(t - base_time, 2) for t in w.time_connected + w.time_disconnected]
            boundary_events = [(t, 0) for t in boundary_times]

        all_events = events + boundary_events
        df = pd.DataFrame(all_events, columns=['time', 'delta'])
        df = df.groupby('time', as_index=False)['delta'].sum()
        df['cumulative'] = df['delta'].cumsum().clip(lower=0)

        if df['cumulative'].isna().all():
            continue

        worker_keys.append(worker)
        compressed_points = compress_time_based_critical_points(df[['time', 'cumulative']].values.tolist())
        raw_points_array.append(compressed_points)


    return worker_keys, raw_points_array


@worker_transfers_bp.route('/worker-incoming-transfers')
@check_and_reload_data()
def get_worker_incoming_transfers():
    try:
        worker_keys, raw_points_array = get_worker_transfer_raw_points('incoming')
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
        worker_keys, raw_points_array = get_worker_transfer_raw_points('outgoing')
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

def export_worker_transfer_csv(role):
    try:
        worker_keys, raw_points_array = get_worker_transfer_raw_points(role)
        if not raw_points_array:
            return jsonify({'error': f'No {role} transfer data'}), 404

        column_data = {}
        time_set = set()

        for worker, points in zip(worker_keys, raw_points_array):
            wid = f"{worker[0]}:{worker[1]}"
            col_map = {floor_decimal(t, 2): v for t, v in points}
            column_data[wid] = col_map
            time_set.update(col_map)

        sorted_times = sorted(time_set)
        columns = list(column_data.keys())

        def generate_csv():
            yield "time (s)," + ",".join(columns) + "\n"
            for t in sorted_times:
                row = [f"{t:.2f}"] + [f"{column_data[c].get(t, 0):.6f}" for c in columns]
                yield ",".join(row) + "\n"

        return Response(
            generate_csv(),
            headers={
                "Content-Disposition": f"attachment; filename=worker_{role}_transfers.csv",
                "Content-Type": "text/csv"
            }
        )
    except Exception as e:
        runtime_state.log_error(f"Error in export_worker_{role}_transfers_csv: {e}")
        return jsonify({'error': str(e)}), 500

@worker_transfers_bp.route('/worker-incoming-transfers/export-csv')
@check_and_reload_data()
def export_worker_incoming_transfers_csv():
    return export_worker_transfer_csv('incoming')

@worker_transfers_bp.route('/worker-outgoing-transfers/export-csv')
@check_and_reload_data()
def export_worker_outgoing_transfers_csv():
    return export_worker_transfer_csv('outgoing')
