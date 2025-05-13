from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_time_formatter,
    d3_size_formatter,
    downsample_points,
    get_unit_and_scale_by_max_file_size_mb
)
from flask import Blueprint, jsonify
import pandas as pd

file_created_size_bp = Blueprint('file_created_size', __name__, url_prefix='/api')

@file_created_size_bp.route('/file-created-size')
@check_and_reload_data()
def get_file_created_size():
    try:
        base_time = runtime_state.MIN_TIME
        files = runtime_state.files
        events = []
        for file in files.values():
            if not file.producers:
                continue

            first_stage_in = None
            for transfer in file.transfers:
                if transfer.time_stage_in is not None:
                    t = float(transfer.time_stage_in - base_time)
                    if first_stage_in is None or t < first_stage_in:
                        first_stage_in = t
            if first_stage_in is not None:
                events.append((first_stage_in, file.size_mb))
        if not events:
            return jsonify({'points': [], 'x_domain': [0, 1], 'y_domain': [0, 0],
                            'x_tick_values': compute_linear_tick_values([0, 1]),
                            'y_tick_values': compute_linear_tick_values([0, 0]),
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
        points = [[t, y * scale] for t, y in points]
        y_domain = [0, y_max * scale]
        points = downsample_points(points, SAMPLING_POINTS)
        return jsonify({
            'points': points,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_size_formatter(unit)
        })
    except Exception as e:
        runtime_state.log_error(f"Error in get_file_created_size: {e}")
        return jsonify({'error': str(e)}), 500 