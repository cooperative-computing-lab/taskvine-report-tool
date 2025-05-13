from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_time_formatter,
    d3_int_formatter,
    downsample_points,
    compute_points_domain
)
from flask import Blueprint, jsonify

task_retrieval_time_bp = Blueprint('task_retrieval_time', __name__, url_prefix='/api')

@task_retrieval_time_bp.route('/task-retrieval-time')
@check_and_reload_data()
def get_task_retrieval_time():
    try:
        raw_points = []

        for idx, task in enumerate(runtime_state.tasks.values()):
            if not task.when_retrieved or not task.when_waiting_retrieval:
                continue
            retrieval_time = round(task.when_retrieved - task.when_waiting_retrieval, 2)
            retrieval_time = max(retrieval_time, 0.01)
            raw_points.append([idx, retrieval_time])

        if not raw_points:
            return jsonify({'error': 'No task retrieval time data available'}), 404

        x_domain, y_domain = compute_points_domain(raw_points)

        return jsonify({
            'points': downsample_points(raw_points, SAMPLING_POINTS),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_time_formatter()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
