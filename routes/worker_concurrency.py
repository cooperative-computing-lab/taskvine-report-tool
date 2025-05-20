from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import compute_linear_tick_values, d3_time_formatter, d3_int_formatter, downsample_points
from flask import Blueprint, jsonify, make_response, Response
import pandas as pd
from io import StringIO

worker_concurrency_bp = Blueprint('worker_concurrency', __name__, url_prefix='/api')

def get_worker_concurrency_points():
    base_time = runtime_state.MIN_TIME
    connect_times = []
    disconnect_times = []

    for worker in runtime_state.workers.values():
        connect_times.extend([round(t - base_time, 2) for t in worker.time_connected])
        disconnect_times.extend([round(t - base_time, 2) for t in worker.time_disconnected])

    initial_active = sum(t <= 0 for t in connect_times)

    events = (
        [(t, 1) for t in connect_times if t > 0] +
        [(t, -1) for t in disconnect_times if t > 0]
    )

    if not events and initial_active == 0:
        return []

    df = pd.DataFrame(events, columns=["time", "delta"])
    df = df.groupby("time", as_index=False)["delta"].sum().sort_values("time")

    df.loc[-1] = [0.0, 0]
    df = df.sort_index().reset_index(drop=True)

    df["active"] = df["delta"].cumsum() + initial_active
    result = df[["time", "active"]].values.tolist()

    max_time = round(runtime_state.MAX_TIME - runtime_state.MIN_TIME, 2)
    if result[-1][0] < max_time:
        result.append([max_time, result[-1][1]])

    return result


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

        def generate_csv():
            yield "time (s),Active Workers\n"
            for t, v in points:
                yield f"{t:.2f},{v}\n"

        return Response(
            generate_csv(),
            headers={
                "Content-Disposition": "attachment; filename=worker_concurrency.csv",
                "Content-Type": "text/csv"
            }
        )

    except Exception as e:
        runtime_state.log_error(f"Error in export_worker_concurrency_csv: {e}")
        return jsonify({'error': str(e)}), 500