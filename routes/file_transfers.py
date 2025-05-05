from .runtime_state import *

file_transfers_bp = Blueprint('file_transfers', __name__, url_prefix='/api')


def downsample_file_transfers(points):
    # downsample transfer data points while keeping the global peak and randomly sampling other points
    if len(points) <= SAMPLING_POINTS:
        return points

    global_peak_idx = max(range(len(points)), key=lambda i: points[i][1])
    global_peak = points[global_peak_idx]

    keep_indices = {0, len(points) - 1, global_peak_idx}

    remaining_points = SAMPLING_POINTS - len(keep_indices)
    if remaining_points <= 0:
        return [points[0], global_peak, points[-1]]

    available_indices = list(set(range(len(points))) - keep_indices)
    sampled_indices = random.sample(available_indices, min(remaining_points, len(available_indices)))
    keep_indices.update(sampled_indices)

    result = [points[i] for i in sorted(keep_indices)]
    return result

@file_transfers_bp.route('/file-transfers')
@check_and_reload_data()
def get_file_transfers():
    try:
        # Get the transfer type from query parameters
        transfer_type = request.args.get('type', 'incoming')  # default to incoming
        if transfer_type not in ['incoming', 'outgoing']:
            return jsonify({'error': 'Invalid transfer type'}), 400

        data = {}

        # construct the file transfers
        data['transfers'] = defaultdict(list)     # for destinations/sources
        for file in runtime_state.files.values():
            for transfer in file.transfers:
                destination = transfer.destination
                source = transfer.source

                # only consider file transfers
                if not isinstance(destination, tuple) or not isinstance(source, tuple):
                    continue

                # if transfer_type is incoming, process destinations
                if transfer_type == 'incoming' and isinstance(destination, tuple):
                    data['transfers'][destination].append((round(transfer.time_start_stage_in - runtime_state.MIN_TIME, 2), 1))
                    if transfer.time_stage_in:
                        data['transfers'][destination].append((round(transfer.time_stage_in - runtime_state.MIN_TIME, 2), -1))
                    elif transfer.time_stage_out:
                        data['transfers'][destination].append((round(transfer.time_stage_out - runtime_state.MIN_TIME, 2), -1))
                # if transfer_type is outgoing, process sources
                elif transfer_type == 'outgoing' and isinstance(source, tuple):
                    data['transfers'][source].append((round(transfer.time_start_stage_in - runtime_state.MIN_TIME, 2), 1))
                    if transfer.time_stage_in:
                        data['transfers'][source].append((round(transfer.time_stage_in - runtime_state.MIN_TIME, 2), -1))
                    elif transfer.time_stage_out:
                        data['transfers'][source].append((round(transfer.time_stage_out - runtime_state.MIN_TIME, 2), -1))

        max_transfers = 0
        for worker in data['transfers']:
            df = pd.DataFrame(data['transfers'][worker], columns=['time', 'event'])
            df = df.sort_values(by=['time'])
            df['cumulative_transfers'] = df['event'].cumsum()
            # if two rows have the same time, keep the one with the largest event
            df = df.drop_duplicates(subset=['time'], keep='last')
            
            # Convert to list of points and downsample
            points = df[['time', 'cumulative_transfers']].values.tolist()
            points = downsample_file_transfers(points)
            data['transfers'][worker] = points
            
            # append the initial point at time_connected with 0
            for time_connected, time_disconnected in zip(runtime_state.workers[worker].time_connected, runtime_state.workers[worker].time_disconnected):
                data['transfers'][worker].insert(0, [time_connected - runtime_state.MIN_TIME, 0])
                data['transfers'][worker].append([time_disconnected - runtime_state.MIN_TIME, 0])
            max_transfers = max(max_transfers, max(point[1] for point in points))

        # convert keys to string-formatted keys
        data['transfers'] = {f"{k[0]}:{k[1]}": v for k, v in data['transfers'].items()}

        # ploting parameters
        data['xMin'] = 0
        data['xMax'] = runtime_state.MAX_TIME - runtime_state.MIN_TIME
        data['yMin'] = 0
        data['yMax'] = int(max_transfers)
        data['xTickValues'] = [
            round(data['xMin'], 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.25, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.5, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.75, 2),
            round(data['xMax'], 2)
        ]
        data['yTickValues'] = [
            int(data['yMin']),
            int(data['yMin'] + (data['yMax'] - data['yMin']) * 0.25),
            int(data['yMin'] + (data['yMax'] - data['yMin']) * 0.5),
            int(data['yMin'] + (data['yMax'] - data['yMin']) * 0.75),
            int(data['yMax'])
        ]
        data['tickFontSize'] = int(runtime_state.tick_size)
        return jsonify(data)

    except Exception as e:
        print(f"Error in get_file_transfers: {str(e)}")
        return jsonify({'error': str(e)}), 500