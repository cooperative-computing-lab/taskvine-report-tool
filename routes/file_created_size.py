from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_time_formatter,
    d3_size_formatter,
    downsample_points,
    get_unit_and_scale_by_max_file_size_mb
)
from flask import Blueprint, jsonify, make_response
from io import StringIO
import pandas as pd

file_created_size_bp = Blueprint('file_created_size', __name__, url_prefix='/api')

def get_file_created_size_points():
    base_time = runtime_state.MIN_TIME
    events = []

    for file in runtime_state.files.values():
        if not file.producers:
            continue

        first_stage_in = None
        for t in file.transfers:
            if t.time_stage_in is not None:
                ts = float(t.time_stage_in - base_time)
                if first_stage_in is None or ts < first_stage_in:
                    first_stage_in = ts
        if first_stage_in is not None:
            events.append((first_stage_in, file.size_mb))

    if not events:
        return [], [0, 1], [0, 0], 'MB', 1.0, pd.DataFrame()

    events.sort()
    cumulative = 0.0
    raw_points = []
    for t, size in events:
        cumulative += size
        raw_points.append([t, cumulative])

    x_domain = [0, max(p[0] for p in raw_points)]
    y_max = max(p[1] for p in raw_points)
    unit, scale = get_unit_and_scale_by_max_file_size_mb(y_max)
    scaled_points = [[x, y * scale] for x, y in raw_points]
    y_domain = [0, y_max * scale]

    df = pd.DataFrame(raw_points, columns=['Time (s)', f'Cumulative Size ({unit})'])
    df[f'Cumulative Size ({unit})'] = df[f'Cumulative Size ({unit})'] * scale

    return scaled_points, x_domain, y_domain, unit, scale, df

@file_created_size_bp.route('/file-created-size')
@check_and_reload_data()
def get_file_created_size():
    try:
        points, x_domain, y_domain, unit, _, _ = get_file_created_size_points()

        return jsonify({
            'points': downsample_points(points, SAMPLING_POINTS),
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


@file_created_size_bp.route('/file-created-size/export-csv')
@check_and_reload_data()
def export_file_created_size_csv():
    try:
        _, _, _, unit, _, df = get_file_created_size_points()
        if df.empty:
            return jsonify({'error': 'No file creation data'}), 404

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=file_created_size.csv'
        response.headers['Content-Type'] = 'text/csv'
        return response
    except Exception as e:
        runtime_state.log_error(f"Error in export_file_created_size_csv: {e}")
        return jsonify({'error': str(e)}), 500