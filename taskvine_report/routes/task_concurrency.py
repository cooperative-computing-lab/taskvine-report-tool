from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app

task_concurrency_bp = Blueprint(
    'task_concurrency', __name__, url_prefix='/api')

@task_concurrency_bp.route('/task-concurrency')
@check_and_reload_data()
def get_task_concurrency():
    try:
        df = read_csv_to_fd(current_app.config["RUNTIME_STATE"].csv_file_task_concurrency)
        phase_data = {}
        for phase in ['Waiting', 'Committing', 'Executing', 'Retrieving', 'Done']:
            phase_data[f"tasks_{phase.lower()}"] = extract_points_from_df(df, 'Time (s)', phase)
        x_domain, y_domain = extract_xy_domains_from_series_points(phase_data)

        return jsonify({
            **phase_data,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_task_concurrency: {e}")
        return jsonify({'error': str(e)}), 500
