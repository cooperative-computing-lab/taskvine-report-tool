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

        # collect execution time data with start time for sorting
        task_data = []
        for task in runtime_state.tasks.values():
            # skip incomplete tasks
            if task.task_status != 0:
                continue
            task_execution_time = round(
                task.time_worker_end - task.time_worker_start, 2)
            # set minimum execution time to 0.01
            task_execution_time = max(task_execution_time, 0.01)
            task_data.append(
                (task.task_id, task_execution_time, task.time_worker_start))

        # sort by start time
        task_data.sort(key=lambda x: x[2])
        
        # prepare data for visualization
        task_execution_time_list = [(task_id, execution_time) for task_id, execution_time, _ in task_data]

        # use full dataset for now
        data['task_execution_time'] = task_execution_time_list

        # calculate CDF
        execution_times = [x[1] for x in task_execution_time_list]
        execution_times_sorted = sorted(execution_times)
        
        # generate evenly spaced probability values
        cdf_values = np.linspace(0, 1, len(execution_times_sorted))
        cdf_points = [(execution_time, round(prob, 4)) for execution_time, prob in zip(execution_times_sorted, cdf_values)]
        
        data['task_execution_time_cdf'] = cdf_points

        # create tick values for axes
        num_tasks = len(task_execution_time_list)
        data['execution_time_x_tick_values'] = [
            1,
            round(num_tasks * 0.25, 2),
            round(num_tasks * 0.5, 2),
            round(num_tasks * 0.75, 2),
            num_tasks
        ]

        max_execution_time = max(x[1] for x in task_execution_time_list)
        data['execution_time_y_tick_values'] = [
            0,
            round(max_execution_time * 0.25, 2),
            round(max_execution_time * 0.5, 2),
            round(max_execution_time * 0.75, 2),
            max_execution_time
        ]

        data['probability_y_tick_values'] = [0, 0.25, 0.5, 0.75, 1]
        data['probability_x_tick_values'] = [
            0,
            round(max_execution_time * 0.25, 2),
            round(max_execution_time * 0.5, 2),
            round(max_execution_time * 0.75, 2),
            max_execution_time
        ]

        data['tickFontSize'] = runtime_state.tick_size

        return jsonify(data)

    except Exception as e:
        print(f"Error in get_task_execution_time: {str(e)}")
        return jsonify({'error': str(e)}), 500
