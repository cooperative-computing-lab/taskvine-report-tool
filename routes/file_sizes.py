from .runtime_state import *

file_sizes_bp = Blueprint('file_sizes', __name__, url_prefix='/api')


def downsample_file_sizes(points):
    # downsample file sizes data points while keeping the global peak and randomly sampling other points
    if len(points) <= SAMPLING_POINTS:
        return points

    # Find global peak (maximum file size)
    global_peak_idx = max(range(len(points)), key=lambda i: points[i][2])  # points[i][2] is file_size
    global_peak = points[global_peak_idx]

    # Find x-axis maximum (latest file)
    x_max_idx = max(range(len(points)), key=lambda i: points[i][3])  # points[i][3] is file_created_time
    x_max_point = points[x_max_idx]

    # Keep the first point, last point, global peak, and x-axis maximum
    keep_indices = {0, len(points) - 1, global_peak_idx, x_max_idx}

    # Calculate how many points we need to keep between each key point
    remaining_points = SAMPLING_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, x_max_point, points[-1]]

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
            sampled_indices = random.sample(available_indices, min(current_gap_points, len(available_indices)))
            keep_indices.update(sampled_indices)
    
    # Sort all indices and return the corresponding points
    result = [points[i] for i in sorted(keep_indices)]
    return result

@file_sizes_bp.route('/file-sizes')
@check_and_reload_data()
def get_file_sizes():
    try:
        # Get the transfer type from query parameters
        order = request.args.get('order', 'asc')  # default to ascending
        file_type = request.args.get('type', 'all')  # default to all
        if order not in ['asc', 'desc', 'created-time']:
            return jsonify({'error': 'Invalid order'}), 400
        if file_type not in ['temp', 'meta', 'buffer', 'task-created', 'transferred', 'all']:
            return jsonify({'error': 'Invalid file type'}), 400
        
        data = {}
        
        # Get the file size of each file
        data['file_sizes'] = []
        max_file_size_mb = 0
        for file in runtime_state.files.values():
            # skip if the file was not staged in at all (outfile of a task but task unsuccessful)
            if len(file.transfers) == 0:
                continue
            file_name = file.filename
            file_size = file.size_mb
            if file_type != 'all':
                if file_type == 'temp' and not file_name.startswith('temp-'):
                    continue
                if file_type == 'meta' and not file_name.startswith('file-meta'):
                    continue
                if file_type == 'buffer' and not file_name.startswith('buffer-'):
                    continue
                if file_type == 'task-created' and len(file.producers) == 0:
                    continue
                if file_type == 'transferred' and len(file.transfers) == 0:
                    continue
            file_created_time = float('inf')
            for transfer in file.transfers:
                file_created_time = round(min(file_created_time, transfer.time_start_stage_in - runtime_state.MIN_TIME), 2)
            if file_created_time == float('inf'):
                print(f"Warning: file {file_name} has no transfer")
            data['file_sizes'].append((0, file_name, file_size, file_created_time))
            max_file_size_mb = max(max_file_size_mb, file_size)

        # sort the file sizes using pandas
        df = pd.DataFrame(data['file_sizes'], columns=['file_idx', 'file_name', 'file_size', 'file_created_time'])
        if order == 'asc':
            df = df.sort_values(by=['file_size'])
        elif order == 'desc':
            df = df.sort_values(by=['file_size'], ascending=False)
        elif order == 'created-time':
            df = df.sort_values(by=['file_created_time'])

        # file idx should start from 1
        df['file_idx'] = range(1, len(df) + 1)
        # convert the file size to the desired unit
        data['file_size_unit'], scale = get_unit_and_scale_by_max_file_size_mb(max_file_size_mb)
        df['file_size'] = df['file_size'] * scale
        
        # Convert to list of points and downsample
        points = df.values.tolist()
        points = downsample_file_sizes(points)
        data['file_sizes'] = points

        # ploting parameters
        if len(points) == 0:
            data['xMin'] = 1
            data['xMax'] = 1
            data['yMin'] = 0    
            data['yMax'] = 0
        else:
            data['xMin'] = 1
            data['xMax'] = len(df)  # Use original length for x-axis
            data['yMin'] = 0    
            data['yMax'] = max_file_size_mb * scale  # Use original max for y-axis
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
        data['file_size_unit'] = data['file_size_unit']

        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500