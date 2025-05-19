from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import compute_linear_tick_values, d3_time_formatter, d3_int_formatter, downsample_points
from flask import Blueprint, jsonify, make_response
import pandas as pd
from io import StringIO

worker_concurrency_bp = Blueprint('worker_concurrency', __name__, url_prefix='/api')

def get_worker_concurrency_points():
    events = []
    initial_active = 0

    for worker in runtime_state.workers.values():
        for t in worker.time_connected:
            if t <= runtime_state.MIN_TIME:
                initial_active += 1
            else:
                events.append((t - runtime_state.MIN_TIME, 1))
        for t in worker.time_disconnected:
            if t > runtime_state.MIN_TIME:
                events.append((t - runtime_state.MIN_TIME, -1))

    if not events and initial_active == 0:
        return []

    events.sort()
    points = [[0, initial_active]]
    active = initial_active
    last_time = 0

    for time, delta in events:
        if time != last_time:
            points.append([time, active])
            last_time = time
        active += delta
        points.append([time, active])

    max_time = runtime_state.MAX_TIME - runtime_state.MIN_TIME
    if points[-1][0] < max_time:
        points.append([max_time, points[-1][1]])

    return points

@worker_concurrency_bp.route('/worker-concurrency')
@check_and_reload_data()
def get_worker_concurrency():
    try:
        points = get_worker_concurrency_points()

        if not points:
            return jsonify({'error': 'No worker concurrency data available'}), 404

        points = downsample_points(points, SAMPLING_POINTS)
        y_max = max(p[1] for p in points)
        x_domain = [0, runtime_state.MAX_TIME - runtime_state.MIN_TIME]
        y_domain = [0, y_max]

        return jsonify({
            'points': points,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter()
        })
    except Exception as e:
        runtime_state.log_error(f"Error in get_worker_concurrency: {e}")
        return jsonify({'error': str(e)}), 500


@worker_concurrency_bp.route('/worker-concurrency/export-csv')
@check_and_reload_data()
def export_worker_concurrency_csv():
    try:
        points = get_worker_concurrency_points()

        if not points:
            return jsonify({'error': 'No worker concurrency data available'}), 404

        df = pd.DataFrame(points, columns=["time (s)", "Active Workers"])
        df = df.round(2)

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=worker_concurrency.csv"
        response.headers["Content-Type"] = "text/csv"
        return response

    except Exception as e:
        runtime_state.log_error(f"Error in export_worker_concurrency_csv: {e}")
        return jsonify({'error': str(e)}), 500