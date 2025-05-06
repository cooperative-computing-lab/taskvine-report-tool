from .runtime_state import runtime_state, SAMPLING_POINTS, check_and_reload_data

import pandas as pd
import random
from flask import Blueprint, jsonify

task_execution_time_bp = Blueprint(
    'task_execution_time', __name__, url_prefix='/api')


def downsample_task_execution_time(points):
    # downsample task execution time points while keeping the first point, last point, and peak execution time
    if len(points) <= SAMPLING_POINTS:
        return points

    # Find global peak (maximum execution time)
    # points[i][1] is execution_time
    global_peak_idx = max(range(len(points)), key=lambda i: points[i][1])
    global_peak = points[global_peak_idx]

    # Keep the first point, last point, and global peak
    keep_indices = {0, len(points) - 1, global_peak_idx}

    # Calculate remaining points to sample
    remaining_points = SAMPLING_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, points[-1]]

    # Sort the indices we want to keep to find gaps between them
    sorted_keep_indices = sorted(keep_indices)

    # Calculate points to keep in each gap
    points_per_gap = remaining_points // (len(sorted_keep_indices) - 1)
    extra_points = remaining_points % (len(sorted_keep_indices) - 1)

    # For each gap between key points, randomly sample points
    for i in range(len(sorted_keep_indices) - 1):
        start_idx = sorted_keep_indices[i]
        end_idx = sorted_keep_indices[i + 1]
        gap_size = end_idx - start_idx - 1

        if gap_size <= 0:
            continue

        # Calculate how many points to keep in this gap
        current_gap_points = points_per_gap
        if extra_points > 0:
            current_gap_points += 1
            extra_points -= 1

        if current_gap_points > 0:
            # Randomly sample points from this gap
            available_indices = list(range(start_idx + 1, end_idx))
            sampled_indices = random.sample(available_indices, min(
                current_gap_points, len(available_indices)))
            keep_indices.update(sampled_indices)

    # Sort all indices and return the corresponding points
    result = [points[i] for i in sorted(keep_indices)]
    return result


@task_execution_time_bp.route('/task-execution-time')
@check_and_reload_data()
def get_task_execution_time():
    try:
        data = {}

        # get the execution time of each task
        task_execution_time_list = []
        for task in runtime_state.tasks.values():
            # skip if the task didn't run to completion
            if task.task_status != 0:
                continue
            task_execution_time = round(
                task.time_worker_end - task.time_worker_start, 2)
            # if a task completes very quickly, we set it to 0.01
            task_execution_time = max(task_execution_time, 0.01)
            task_execution_time_list.append(
                (task.task_id, task_execution_time))

        # sort by execution time for better visualization
        task_execution_time_list.sort(key=lambda x: x[1])

        # downsample the data points
        # data['task_execution_time'] = downsample_task_execution_time(task_execution_time_list)
        data['task_execution_time'] = task_execution_time_list

        # calculate the cdf using the original (non-downsampled) data to maintain accuracy
        df = pd.DataFrame(task_execution_time_list, columns=[
                          'task_id', 'task_execution_time'])
        df['cumulative_execution_time'] = df['task_execution_time'].cumsum()
        df['probability'] = df['cumulative_execution_time'] / \
            df['cumulative_execution_time'].max()
        df['probability'] = df['probability'].round(4)

        # downsample the CDF data points as well
        cdf_points = df[['task_execution_time', 'probability']].values.tolist()
        # data['task_execution_time_cdf'] = downsample_task_execution_time(cdf_points)
        data['task_execution_time_cdf'] = cdf_points

        # tick values - use original data ranges to maintain proper axis scaling
        num_tasks = len(task_execution_time_list)  # use original length
        data['execution_time_x_tick_values'] = [
            1,
            round(num_tasks * 0.25, 2),
            round(num_tasks * 0.5, 2),
            round(num_tasks * 0.75, 2),
            num_tasks
        ]

        max_execution_time = max(
            x[1] for x in task_execution_time_list)  # use original max
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
