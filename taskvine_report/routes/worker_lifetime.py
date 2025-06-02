from .utils import *
from flask import Blueprint, jsonify, make_response, current_app
from io import StringIO
import pandas as pd

worker_lifetime_bp = Blueprint('worker_lifetime', __name__, url_prefix='/api')

def get_worker_lifetime_points():
    entries = []
    for worker in current_app.config["RUNTIME_STATE"].workers.values():
        worker_key = worker.get_worker_key()
        worker_id = worker.id
        for i, t_start in enumerate(worker.time_connected):
            t_end = (
                worker.time_disconnected[i]
                if i < len(worker.time_disconnected)
                else current_app.config["RUNTIME_STATE"].MAX_TIME
            )
            t0 = round(max(0, t_start - current_app.config["RUNTIME_STATE"].MIN_TIME), 2)
            t1 = round(max(0, t_end - current_app.config["RUNTIME_STATE"].MIN_TIME), 2)
            duration = round(max(0, t1 - t0), 2)
            entries.append((t0, duration, worker_id, worker_key))

    entries.sort(key=lambda x: x[0])
    points = [[worker_id, duration] for _, duration, worker_id, _ in entries]
    idx_to_worker_key = {worker_id: worker_key for _, _, worker_id, worker_key in entries}
    return points, idx_to_worker_key

@worker_lifetime_bp.route('/worker-lifetime')
@check_and_reload_data()
def get_worker_lifetime():
    try:
        points, idx_to_worker_key = get_worker_lifetime_points()

        if not points:
            return jsonify({'error': 'No worker lifetime data'}), 404

        x_domain = [p[0] for p in points]
        y_domain = [0, max((p[1] for p in points), default=1)]

        return jsonify({
            'points': points,
            'idx_to_worker_key': idx_to_worker_key,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_discrete_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_time_formatter()
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_worker_lifetime: {e}")
        return jsonify({'error': str(e)}), 500


@worker_lifetime_bp.route('/worker-lifetime/export-csv')
@check_and_reload_data()
def export_worker_lifetime_csv():
    try:
        points, idx_to_worker_key = get_worker_lifetime_points()
        if not points:
            return jsonify({'error': 'No worker lifetime data'}), 404

        df = pd.DataFrame(points, columns=["ID", "Lifetime"])
        df["Worker IP Port"] = df["ID"].map(idx_to_worker_key)
        df["Worker IP Port"] = df["Worker IP Port"].map(get_worker_ip_port_from_key)
        df = df[["ID", "Worker IP Port", "Lifetime"]].round(2)
        df.columns = ["ID", "Worker IP Port", "Lifetime (s)"]

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        response = make_response(buffer.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=worker_lifetime.csv"
        response.headers["Content-Type"] = "text/csv"
        return response

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_worker_lifetime_csv: {e}")
        return jsonify({'error': str(e)}), 500
