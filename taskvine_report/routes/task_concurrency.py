from taskvine_report.utils import *
from flask import Blueprint, jsonify, current_app, request

task_concurrency_bp = Blueprint(
    'task_concurrency', __name__, url_prefix='/api')

@task_concurrency_bp.route('/task-concurrency')
@check_and_reload_data()
def get_task_concurrency():
    try:
        # check if recovery-task-only parameter is set
        recovery_only = request.args.get('recovery-task-only', 'false').lower() == 'true'
        
        # select appropriate CSV file based on parameter
        csv_file = (current_app.config["RUNTIME_STATE"].csv_file_task_concurrency_recovery_only 
                   if recovery_only 
                   else current_app.config["RUNTIME_STATE"].csv_file_task_concurrency)
        
        df = read_csv_to_fd(csv_file)
        phase_data = {}
        for phase in ['Waiting', 'Committing', 'Executing', 'Retrieving', 'Done']:
            phase_points = extract_points_from_df(df, 'Time (s)', phase)
            phase_data[f"tasks_{phase.lower()}"] = downsample_points(phase_points)

        x_domain = get_current_time_domain()
        y_domain = extract_y_range_from_series_points(phase_data)

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
