from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_time_formatter,
    d3_int_formatter,
    downsample_points,
    compute_points_domain
)

from flask import Blueprint, jsonify

task_response_time_bp = Blueprint(
    'task_response_time', __name__, url_prefix='/api')

@task_response_time_bp.route('/task-response-time')
@check_and_reload_data()
def get_task_response_time():
    try:
        raw_points = []

        for idx, task in enumerate(runtime_state.tasks.values()):
            if not task.when_running:
                continue
            response_time = max(round(task.when_running - task.when_ready, 2), 0.01)
            raw_points.append([idx, response_time])

        if not raw_points:
            return jsonify({'error': 'No task has started running'}), 404

        x_domain, y_domain = compute_points_domain(raw_points)

        points = downsample_points(raw_points, SAMPLING_POINTS)

        return jsonify({
            'points': points,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_time_formatter()
        })

    except Exception as e:
        runtime_state.log_error(f"Error in get_task_response_time: {e}")
        return jsonify({'error': str(e)}), 500
