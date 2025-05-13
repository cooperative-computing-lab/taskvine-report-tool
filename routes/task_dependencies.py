from flask import Blueprint, jsonify
from .runtime_state import runtime_state
from .utils import (
    compute_linear_tick_values,
    d3_int_formatter,
    compute_points_domain,
    compute_task_dependency_metrics
)

task_dependencies_bp = Blueprint('task_dependencies', __name__, url_prefix='/api')

@task_dependencies_bp.route('/task-dependencies')
def get_task_dependencies():
    try:
        points = compute_task_dependency_metrics(runtime_state.tasks, mode='dependencies')
        x_domain, y_domain = compute_points_domain(points)

        return jsonify({
            'points': points,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain, round_digits=0, num_ticks=10),
            'x_tick_formatter': d3_int_formatter(),
            'y_tick_formatter': d3_int_formatter()
        })

    except Exception as e:
        runtime_state.log_error(f"Error in get_task_dependencies: {e}")
        return jsonify({'error': str(e)}), 500
