from .runtime_state import *

file_replicas_bp = Blueprint('file_replicas', __name__, url_prefix='/api')


def downsample_file_replicas(points):
    # downsample file replicas data points while keeping the global peak and randomly sampling other points
    if len(points) <= TARGET_POINTS:
        return points

    # Find global peak (maximum number of replicas)
    global_peak_idx = max(range(len(points)), key=lambda i: points[i][3])  # points[i][3] is num_replicas
    global_peak = points[global_peak_idx]

    # Keep the first point, last point, and global peak
    keep_indices = {0, len(points) - 1, global_peak_idx}

    # Calculate how many points we need to keep between each key point
    remaining_points = TARGET_POINTS - len(keep_indices)
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
            sampled_indices = random.sample(available_indices, min(current_gap_points, len(available_indices)))
            keep_indices.update(sampled_indices)
    
    # Sort all indices and return the corresponding points
    result = [points[i] for i in sorted(keep_indices)]
    return result

@file_replicas_bp.route('/file-replicas')
@check_and_reload_data()
def get_file_replicas():
    try:
        order = request.args.get('order', 'desc')  # default to descending
        if order not in ['asc', 'desc']:
            return jsonify({'error': 'Invalid order'}), 400

        data = {}
        
        # Get the file size of each file
        data['file_replicas'] = []
        for file in runtime_state.files.values():
            # skip if the file was not staged in at all (outfile of a task but task unsuccessful)
            file_name = file.filename
            file_size = file.size_mb
            if len(file.transfers) == 0:
                continue
            # skip if not a temp file
            if not file_name.startswith('temp-'):
                continue
            workers = set()
            for transfer in file.transfers:
                # skip if the file is not staged in
                if not transfer.time_stage_in:
                    continue
                workers.add(transfer.destination)
            data['file_replicas'].append((0, file_name, file_size, len(workers)))

        # sort the file replicas using pandas
        df = pd.DataFrame(data['file_replicas'], columns=['file_idx', 'file_name', 'file_size', 'num_replicas'])
        if order == 'asc':
            df = df.sort_values(by=['num_replicas'])
        elif order == 'desc':   
            df = df.sort_values(by=['num_replicas'], ascending=False)
        df['file_idx'] = range(1, len(df) + 1)

        # convert numpy int to python int
        df['num_replicas'] = df['num_replicas'].astype(int)
        df['file_size'] = df['file_size'].astype(int)

        # Convert to list of points and downsample
        points = df.values.tolist()
        points = downsample_file_replicas(points)
        data['file_replicas'] = points
        
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
            data['yMax'] = int(df['num_replicas'].max())  # Use original max for y-axis
        data['xTickValues'] = [
            round(data['xMin'], 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.25, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.5, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.75, 2),
            round(data['xMax'], 2)
            ]
        data['yTickValues'] = [
            round(data['yMin'], 2),
            int(round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.25, 2)),
            int(round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.5, 2)),
            int(round(data['yMin'] + (data['yMax'] - data['yMin']) * 0.75, 2)),
            int(round(data['yMax'], 2))
            ]
        data['tickFontSize'] = runtime_state.tick_size

        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500