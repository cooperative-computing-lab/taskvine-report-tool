from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

worker_transfers_bp = Blueprint('worker_transfers', __name__, url_prefix='/api')

def _get_worker_transfer_data(role):
    try:
        csv_attr = "csv_file_worker_incoming_transfers" if role == "incoming" else "csv_file_worker_outgoing_transfers"
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].__getattribute__(csv_attr))

        data = extract_series_points_dict(df, 'Time (s)')
        x_domain, y_domain = extract_xy_domains_from_series_points(data)

        return jsonify({
            'transfers': downsample_series_points(data),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_worker_{role}_transfers: {e}")
        return jsonify({'error': str(e)}), 500


@worker_transfers_bp.route('/worker-incoming-transfers')
@check_and_reload_data()
def get_worker_incoming_transfers():
    return _get_worker_transfer_data("incoming")


@worker_transfers_bp.route('/worker-outgoing-transfers')
@check_and_reload_data()
def get_worker_outgoing_transfers():
    return _get_worker_transfer_data("outgoing")
