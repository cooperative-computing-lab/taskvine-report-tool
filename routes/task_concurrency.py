from .runtime_state import *

task_concurrency_bp = Blueprint(
    'task_concurrency', __name__, url_prefix='/api')


def downsample_task_concurrency(points):
    # If points are fewer than target, return all
    if len(points) <= SAMPLING_POINTS:
        return points

    # Find global peak (maximum concurrency)
    # points[i][1] is concurrency
    global_peak_idx = max(range(len(points)), key=lambda i: points[i][1])
    global_peak = points[global_peak_idx]

    # Keep first, last and peak points
    keep_indices = {0, len(points) - 1, global_peak_idx}

    # Calculate remaining points to sample
    remaining_points = SAMPLING_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, points[-1]]

    # Sort key indices to find gaps
    sorted_keep_indices = sorted(keep_indices)

    # Calculate points per gap
    points_per_gap = remaining_points // (len(sorted_keep_indices) - 1)
    extra_points = remaining_points % (len(sorted_keep_indices) - 1)

    # Sample points from each gap
    for i in range(len(sorted_keep_indices) - 1):
        start_idx = sorted_keep_indices[i]
        end_idx = sorted_keep_indices[i + 1]
        gap_size = end_idx - start_idx - 1

        if gap_size <= 0:
            continue

        # Calculate points for this gap
        current_gap_points = points_per_gap
        if extra_points > 0:
            current_gap_points += 1
            extra_points -= 1

        if current_gap_points > 0:
            # Randomly sample from gap
            available_indices = list(range(start_idx + 1, end_idx))
            sampled_indices = random.sample(available_indices, min(
                current_gap_points, len(available_indices)))
            keep_indices.update(sampled_indices)

    # Return sorted points
    result = [points[i] for i in sorted(keep_indices)]
    return result


@task_concurrency_bp.route('/task-concurrency')
@check_and_reload_data()
def get_task_concurrency():
    try:
        data = {}

        # Get selected task types
        selected_types = request.args.get('types', '').split(',')
        if not selected_types or selected_types == ['']:
            selected_types = [
                'tasks_waiting',
                'tasks_committing',
                'tasks_executing',
                'tasks_retrieving',
                'tasks_done'
            ]

        # Initialize task type lists
        all_task_types = {
            'tasks_waiting': [],
            'tasks_committing': [],
            'tasks_executing': [],
            'tasks_retrieving': [],
            'tasks_done': []
        }
        data.update(all_task_types)

        # Process selected task types
        for task in runtime_state.tasks.values():
            if task.when_failure_happens is not None:
                continue

            # Collect task state data
            if 'tasks_waiting' in selected_types and task.when_ready:
                data['tasks_waiting'].append(
                    (task.when_ready - runtime_state.MIN_TIME, 1))
                if task.when_running:
                    data['tasks_waiting'].append(
                        (task.when_running - runtime_state.MIN_TIME, -1))

            if 'tasks_committing' in selected_types and task.when_running:
                data['tasks_committing'].append(
                    (task.when_running - runtime_state.MIN_TIME, 1))
                if task.time_worker_start:
                    data['tasks_committing'].append(
                        (task.time_worker_start - runtime_state.MIN_TIME, -1))

            if 'tasks_executing' in selected_types and task.time_worker_start:
                data['tasks_executing'].append(
                    (task.time_worker_start - runtime_state.MIN_TIME, 1))
                if task.time_worker_end:
                    data['tasks_executing'].append(
                        (task.time_worker_end - runtime_state.MIN_TIME, -1))

            if 'tasks_retrieving' in selected_types and task.time_worker_end:
                data['tasks_retrieving'].append(
                    (task.time_worker_end - runtime_state.MIN_TIME, 1))
                if task.when_retrieved:
                    data['tasks_retrieving'].append(
                        (task.when_retrieved - runtime_state.MIN_TIME, -1))

            if 'tasks_done' in selected_types:
                if task.when_done:
                    data['tasks_done'].append(
                        (task.when_done - runtime_state.MIN_TIME, 1))

        def process_task_type(tasks):
            if not tasks:
                return []
            # Convert to DataFrame and calculate cumulative events
            df = pd.DataFrame(tasks, columns=['time', 'event'])
            df = df.sort_values(by=['time'])
            df['time'] = df['time'].round(2)
            df['cumulative_event'] = df['event'].cumsum()
            # Keep last event for duplicate timestamps
            df = df.drop_duplicates(subset=['time'], keep='last')
            points = df[['time', 'cumulative_event']].values.tolist()
            # Downsample data
            return downsample_task_concurrency(points)

        # Process all task types data
        max_time = float('-inf')
        max_concurrent = 0
        for task_type in all_task_types:
            data[task_type] = process_task_type(data[task_type])
            # Update max values
            if data[task_type]:
                max_time = max(max_time, max(
                    point[0] for point in data[task_type]))
                if task_type in selected_types:
                    max_concurrent = max(max_concurrent, max(
                        point[1] for point in data[task_type]))

        # Set axis ranges
        data['xMin'] = 0
        data['xMax'] = max_time
        data['yMin'] = 0
        data['yMax'] = max_concurrent

        # Generate tick values
        data['xTickValues'] = [
            round(data['xMin'], 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.25, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.5, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.75, 2),
            round(data['xMax'], 2)
        ]
        data['yTickValues'] = [
            round(data['yMin'], 2),
            round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.25, 2),
            round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.5, 2),
            round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.75, 2),
            round(data['yMax'], 2)
        ]
        data['tickFontSize'] = runtime_state.tick_size

        return jsonify(data)
    except Exception as e:
        print(f"Error in get_task_concurrency: {str(e)}")
        return jsonify({'error': str(e)}), 500
