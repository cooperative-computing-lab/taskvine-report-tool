from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import compute_tick_values, d3_time_formatter, d3_int_formatter, downsample_points
from flask import Blueprint, jsonify

worker_concurrency_bp = Blueprint('worker_concurrency', __name__, url_prefix='/api')

@worker_concurrency_bp.route('/worker-concurrency')
@check_and_reload_data()
def get_worker_concurrency():
    try:
        events = []  # (time, +1/-1)
        for worker in runtime_state.workers.values():
            for t in worker.time_connected:
                events.append((t - runtime_state.MIN_TIME, 1))
            for t in worker.time_disconnected:
                events.append((t - runtime_state.MIN_TIME, -1))
        if not events:
            return jsonify({'error': 'No worker concurrency data available'}), 404

        events.sort()
        points = []
        active = 0
        last_time = 0
        for time, delta in events:
            if time != last_time or not points:
                points.append([time, active])
                last_time = time
            active += delta
            points.append([time, active])

        max_time = runtime_state.MAX_TIME - runtime_state.MIN_TIME
        if points[-1][0] < max_time:
            points.append([max_time, points[-1][1]])
        y_max = max(p[1] for p in points)
        x_domain = [0, max_time]
        y_domain = [0, y_max]
        return jsonify({
            'points': downsample_points(points, SAMPLING_POINTS),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_tick_values(x_domain),
            'y_tick_values': compute_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500 