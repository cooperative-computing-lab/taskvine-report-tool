from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

worker_lifetime_bp = Blueprint('worker_lifetime', __name__, url_prefix='/api')

@worker_lifetime_bp.route('/worker-lifetime')
@check_and_reload_data()
def get_worker_lifetime():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_worker_lifetime)
        points = extract_points_from_df(df, 'ID', 'LifeTime (s)')
        x_domain = [p[0] for p in points]
        y_domain = [0, max((p[1] for p in points), default=1)]

        return jsonify({
            'points': downsample_points(points),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_discrete_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_time_formatter(),
            'idx_to_worker_key': {
                row['ID']: row['Worker IP Port'] for _, row in df.iterrows()
            },
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_worker_lifetime: {e}")
        return jsonify({'error': str(e)}), 500
