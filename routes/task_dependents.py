from flask import Blueprint, jsonify
from .runtime_state import runtime_state
from .utils import (
    compute_linear_tick_values,
    d3_int_formatter,
    compute_points_domain,
    compute_task_dependency_metrics
)

task_dependents_bp = Blueprint('task_dependents', __name__, url_prefix='/api')

@task_dependents_bp.route('/task-dependents')
def get_task_dependents():
    points = compute_task_dependency_metrics(runtime_state.tasks, mode='dependents')
    x_domain, y_domain = compute_points_domain(points)

    return jsonify({
        'points': points,
        'x_domain': x_domain,
        'y_domain': y_domain,
        'x_tick_values': compute_linear_tick_values(x_domain),
        'y_tick_values': compute_linear_tick_values(y_domain),
        'x_tick_formatter': d3_int_formatter(),
        'y_tick_formatter': d3_int_formatter()
    })