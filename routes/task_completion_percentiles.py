from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import (
    compute_linear_tick_values,
    d3_time_formatter,
    d3_int_formatter,
    d3_percentage_formatter,
    compute_discrete_tick_values
)
from flask import Blueprint, jsonify
import numpy as np


task_completion_percentiles_bp = Blueprint('task_completion_percentiles', __name__, url_prefix='/api')

@task_completion_percentiles_bp.route('/task-completion-percentiles')
@check_and_reload_data()
def get_task_completion_percentiles():
    try:
        tasks = runtime_state.tasks.values()
        finish_times = [
            (task.when_done or task.when_retrieved) - runtime_state.MIN_TIME
            for task in tasks
            if (task.when_done or task.when_retrieved)
        ]
        x_domain = list(range(1, 101))
        if not finish_times:
            return jsonify({'points': [], 'x_domain': x_domain, 'y_domain': [0, 0],
                            'x_tick_values': compute_discrete_tick_values(x_domain),
                            'y_tick_values': [0],
                            'x_tick_formatter': d3_percentage_formatter(digits=0),
                            'y_tick_formatter': d3_time_formatter()})
        finish_times = sorted(finish_times)
        n = len(finish_times)
        points = []
        for p in range(1, 101):
            idx = int(np.ceil(p / 100 * n)) - 1
            idx = max(0, min(idx, n - 1))
            points.append([p, finish_times[idx]])
        y_max = max(t for _, t in points)
        y_domain = [0, y_max]
        return jsonify({
            'points': points,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_discrete_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_percentage_formatter(digits=0),
            'y_tick_formatter': d3_time_formatter()
        })
    except Exception as e:
        runtime_state.log_error(f"Error in get_task_completion_percentiles: {e}")
        return jsonify({'error': str(e)}), 500 