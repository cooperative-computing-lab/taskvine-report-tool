from .runtime_state import runtime_state, check_and_reload_data, SAMPLING_POINTS
from .utils import (
    compute_linear_tick_values,
    d3_int_formatter,
    d3_time_formatter,
    compute_discrete_tick_values
)
from flask import Blueprint, jsonify

worker_lifetime_bp = Blueprint('worker_lifetime', __name__, url_prefix='/api')

@worker_lifetime_bp.route('/worker-lifetime')
@check_and_reload_data()
def get_worker_lifetime():
    try:
        entries = []

        for worker in runtime_state.workers.values():
            ip_port = worker.get_worker_ip_port()
            worker_id = worker.id
            for i, t_start in enumerate(worker.time_connected):
                t_end = (
                    worker.time_disconnected[i]
                    if i < len(worker.time_disconnected)
                    else runtime_state.MAX_TIME
                )
                t0 = max(0, t_start - runtime_state.MIN_TIME)
                t1 = max(0, t_end - runtime_state.MIN_TIME)
                duration = round(max(0, t1 - t0), 2)

                entries.append((t0, duration, worker_id, ip_port))

        entries.sort(key=lambda x: x[0])

        points = [[worker_id, duration] for _, duration, worker_id, _ in entries]
        idx_to_worker_ip_port = {
            worker_id: ip_port for _, _, worker_id, ip_port in entries
        }

        x_domain = [p[0] for p in points]
        y_domain = [0, max((p[1] for p in points), default=1)]

        return jsonify({
            'points': points,
            'idx_to_worker_ip_port': idx_to_worker_ip_port,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_discrete_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_int_formatter()
        })
    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 500