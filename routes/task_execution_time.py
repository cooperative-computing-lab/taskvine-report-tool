from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data

import pandas as pd
import random
from flask import Blueprint, jsonify
import numpy as np

task_execution_time_bp = Blueprint(
    'task_execution_time', __name__, url_prefix='/api')


def downsample_task_execution_time(points):
    # downsample while keeping first, last, and peak points
    if len(points) <= SAMPLING_POINTS:
        return points

    # find global peak (maximum execution time)
    global_peak_idx = max(range(len(points)), key=lambda i: points[i][1])
    global_peak = points[global_peak_idx]

    # keep key points
    keep_indices = {0, len(points) - 1, global_peak_idx}

    # calculate remaining points to sample
    remaining_points = SAMPLING_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, points[-1]]

    # sort indices to find gaps
    sorted_keep_indices = sorted(keep_indices)

    # calculate points per gap
    points_per_gap = remaining_points // (len(sorted_keep_indices) - 1)
    extra_points = remaining_points % (len(sorted_keep_indices) - 1)

    # for each gap, sample points randomly
    for i in range(len(sorted_keep_indices) - 1):
        start_idx = sorted_keep_indices[i]
        end_idx = sorted_keep_indices[i + 1]
        gap_size = end_idx - start_idx - 1

        if gap_size <= 0:
            continue

        # calculate points for this gap
        current_gap_points = points_per_gap
        if extra_points > 0:
            current_gap_points += 1
            extra_points -= 1

        if current_gap_points > 0:
            # randomly sample from gap
            available_indices = list(range(start_idx + 1, end_idx))
            sampled_indices = random.sample(available_indices, min(
                current_gap_points, len(available_indices)))
            keep_indices.update(sampled_indices)

    # return sorted points
    result = [points[i] for i in sorted(keep_indices)]
    return result


@task_execution_time_bp.route('/task-execution-time')
@check_and_reload_data()
def get_task_execution_time():
    try:
        data = {}

        # calculate execution time for each task
        points = []
        for i, task in enumerate(runtime_state.tasks.values()):
            # skip incomplete tasks
            if task.task_status != 0:
                continue
            task_execution_time = round(
                task.time_worker_end - task.time_worker_start, 2)
            # set minimum execution time to 0.01
            task_execution_time = max(task_execution_time, 0.01)
            points.append([i, task_execution_time])

        # calculate domains and tick values
        x_domain = [0, len(points)]
        y_domain = [0, max(p[1] for p in points) if points else 0]

        x_tick_values = [
            1,
            round(len(points) * 0.25, 2),
            round(len(points) * 0.5, 2),
            round(len(points) * 0.75, 2),
            len(points)
        ]

        y_tick_values = [
            0,
            round(y_domain[1] * 0.25, 2),
            round(y_domain[1] * 0.5, 2),
            round(y_domain[1] * 0.75, 2),
            y_domain[1]
        ]

        return jsonify({
            'points': points,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': x_tick_values,
            'y_tick_values': y_tick_values
        })

    except Exception as e:
        print(f"Error in get_task_execution_time: {str(e)}")
        return jsonify({'error': str(e)}), 500
