import pandas as pd
import os

def file_concurrent_replicas_csv(files, min_time, output_csv_path):
    rows = []

    for file_idx, (fname, file) in enumerate(files.items(), start=1):
        if file.created_time < min_time:
            continue

        intervals = [
            (t.time_stage_in, t.time_stage_out)
            for t in file.transfers
            if t.time_stage_in and t.time_stage_out
        ]
        if not intervals:
            max_simul = 0
        else:
            events = []
            for start, end in intervals:
                events.append((start, 1))
                events.append((end, -1))
            events.sort()
            count = 0
            max_simul = 0
            for _, delta in events:
                count += delta
                max_simul = max(max_simul, count)

        rows.append((file_idx, fname, max_simul, file.created_time))

    if not rows:
        return

    df = pd.DataFrame(rows, columns=['file_idx', 'file_name', 'max_simul_replicas', 'created_time'])
    df = df.sort_values(by='created_time')
    df['file_idx'] = range(1, len(df) + 1)
    df = df[['file_idx', 'file_name', 'max_simul_replicas']]
    df.columns = ['File Index', 'File Name', 'Max Concurrent Replicas (count)']

    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    df.to_csv(output_csv_path, index=False)
