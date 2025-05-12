from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_tick_values,
    d3_time_formatter,
    d3_size_formatter,
    downsample_points,
    get_unit_and_scale_by_max_file_size_mb
)
from flask import Blueprint, jsonify
import pandas as pd

file_transferred_size_bp = Blueprint('file_transferred_size', __name__, url_prefix='/api')

@file_transferred_size_bp.route('/file-transferred-size')
@check_and_reload_data()
def get_file_transferred_size():
    try:
        base_time = runtime_state.MIN_TIME
        files = runtime_state.files
        events = []
        for file in files.values():
            for transfer in file.transfers:
                if transfer.time_stage_in:
                    t = float(transfer.time_stage_in - base_time)
                    events.append((t, file.size_mb))
                elif transfer.time_stage_out:
                    t = float(transfer.time_stage_out - base_time)
                    events.append((t, file.size_mb))
        if not events:
            return jsonify({'points': [], 'x_domain': [0, 1], 'y_domain': [0, 0],
                            'x_tick_values': compute_tick_values([0, 1]),
                            'y_tick_values': compute_tick_values([0, 0]),
                            'x_tick_formatter': d3_time_formatter(),
                            'y_tick_formatter': d3_size_formatter('MB')})

        events.sort()
        points = []
        total = 0.0
        for t, size in events:
            total += size
            points.append([t, total])
        x_domain = [0, max(p[0] for p in points)]
        y_max = max(p[1] for p in points)
        unit, scale = get_unit_and_scale_by_max_file_size_mb(y_max)
        # scale all y values
        points = [[t, y * scale] for t, y in points]
        y_domain = [0, y_max * scale]
        points = downsample_points(points, SAMPLING_POINTS)
        return jsonify({
            'points': points,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_tick_values(x_domain),
            'y_tick_values': compute_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_size_formatter(unit)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500 