from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data
from .utils import compute_tick_values, d3_time_formatter, d3_int_formatter

import pandas as pd
import random
from flask import Blueprint, jsonify
import numpy as np

task_response_time_bp = Blueprint(
    'task_response_time', __name__, url_prefix='/api')


def downsample_task_response_time(points):
    # downsample while keeping first, last, and peak points
    if len(points) <= SAMPLING_POINTS:
        return points

    # find global peak (maximum response time)
    global_peak_idx = max(range(len(points)), key=lambda i: points[i][1])
    global_peak = points[global_peak_idx]

    # keep the first point, last point, and global peak
    keep_indices = {0, len(points) - 1, global_peak_idx}

    # calculate remaining points to sample
    remaining_points = SAMPLING_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, points[-1]]

    # sort the indices we want to keep to find gaps between them
    sorted_keep_indices = sorted(keep_indices)

    # calculate points to keep in each gap
    points_per_gap = remaining_points // (len(sorted_keep_indices) - 1)
    extra_points = remaining_points % (len(sorted_keep_indices) - 1)

    # for each gap between key points, randomly sample points
    for i in range(len(sorted_keep_indices) - 1):
        start_idx = sorted_keep_indices[i]
        end_idx = sorted_keep_indices[i + 1]
        gap_size = end_idx - start_idx - 1

        if gap_size <= 0:
            continue

        # calculate how many points to keep in this gap
        current_gap_points = points_per_gap
        if extra_points > 0:
            current_gap_points += 1
            extra_points -= 1

        if current_gap_points > 0:
            # randomly sample points from this gap
            available_indices = list(range(start_idx + 1, end_idx))
            sampled_indices = random.sample(available_indices, min(
                current_gap_points, len(available_indices)))
            keep_indices.update(sampled_indices)

    # sort all indices and return the corresponding points
    result = [points[i] for i in sorted(keep_indices)]
    return result


@task_response_time_bp.route('/task-response-time')
@check_and_reload_data()
def get_task_response_time():
    try:
        data = {}

        # Calculate response time for each task
        data['points'] = []
        for i, task in enumerate(runtime_state.tasks.values()):
            # skip tasks that haven't started running yet
            if not task.when_running:
                continue
            
            response_time = round(task.when_running - task.when_ready, 2)
            # set minimum response time to 0.01
            response_time = max(response_time, 0.01)
            data['points'].append([i, response_time])


        # Calculate domains and tick values
        data['x_domain'] = [0, len(data['points'])]
        data['y_domain'] = [0, max(p[1] for p in data['points']) if data['points'] else 0]
        data['x_tick_values'] = compute_tick_values(data['x_domain'])
        data['y_tick_values'] = compute_tick_values(data['y_domain'])

        data['x_tick_formatter'] = d3_int_formatter()
        data['y_tick_formatter'] = d3_time_formatter()

        # downsample points
        data['points'] = downsample_task_response_time(data['points'])
        
        return jsonify(data)

    except Exception as e:
        print(f"Error in get_task_response_time: {str(e)}")
        return jsonify({'error': str(e)}), 500 