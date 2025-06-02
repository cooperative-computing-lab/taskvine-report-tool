from .utils import *
from flask import Blueprint, jsonify, Response, current_app
import pandas as pd

file_created_size_bp = Blueprint('file_created_size', __name__, url_prefix='/api')

def get_file_created_size_points():
    base_time = current_app.config["RUNTIME_STATE"].MIN_TIME
    events = []

    for file in current_app.config["RUNTIME_STATE"].files.values():
        if not file.producers:
            continue

        stage_times = [t.time_stage_in for t in file.transfers if t.time_stage_in is not None]
        if stage_times:
            first_time = min(stage_times)
            t = round(float(first_time - base_time), 2)
            events.append((t, file.size_mb))

    if not events:
        return [], [0, 1], [0, 0], 'MB', 1.0, pd.DataFrame()

    df = pd.DataFrame(events, columns=['time', 'delta'])
    df['time'] = df['time'].round(2)
    df = df.groupby('time', as_index=False)['delta'].sum()
    df['cumulative'] = df['delta'].cumsum()
    df['cumulative'] = df['cumulative'].clip(lower=0)

    if df.empty:
        return [], [0, 1], [0, 0], 'MB', 1.0, pd.DataFrame()

    raw_points = df[['time', 'cumulative']].values.tolist()
    x_max = df['time'].max()
    y_max = df['cumulative'].max()

    unit, scale = get_unit_and_scale_by_max_file_size_mb(y_max if pd.notna(y_max) else 0)
    scaled_points = [[x, y * scale] for x, y in raw_points]

    x_domain = [0, float(x_max) if pd.notna(x_max) else 1.0]
    y_domain = [0, y_max * scale if pd.notna(y_max) else 0]

    export_df = pd.DataFrame(raw_points, columns=['Time (s)', f'Cumulative Size ({unit})'])
    export_df[f'Cumulative Size ({unit})'] *= scale
    export_df = export_df.sort_values('Time (s)')

    return scaled_points, x_domain, y_domain, unit, scale, export_df


@file_created_size_bp.route('/file-created-size')
@check_and_reload_data()
def get_file_created_size():
    try:
        points, x_domain, y_domain, unit, _, _ = get_file_created_size_points()

        return jsonify({
            'points': downsample_points(points),
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_size_formatter(unit)
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_file_created_size: {e}")
        return jsonify({'error': str(e)}), 500

@file_created_size_bp.route('/file-created-size/export-csv')
@check_and_reload_data()
def export_file_created_size_csv():
    try:
        _, _, _, unit, scale, df = get_file_created_size_points()
        if df.empty:
            return jsonify({'error': 'No file creation data'}), 404

        def generate_csv():
            yield f"Time (s),Cumulative Size ({unit})\n"
            for t, cumulative in df.values:
                yield f"{t},{cumulative}\n"

        return Response(
            generate_csv(),
            headers={
                "Content-Disposition": "attachment; filename=file_created_size.csv",
                "Content-Type": "text/csv"
            }
        )
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_file_created_size_csv: {e}")
        return jsonify({'error': str(e)}), 500
