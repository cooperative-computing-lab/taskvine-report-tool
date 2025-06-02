from .utils import *
from flask import Blueprint, jsonify, Response, current_app
import pandas as pd

file_transferred_size_bp = Blueprint('file_transferred_size', __name__, url_prefix='/api')

def get_file_transferred_size_points():
    base_time = current_app.config["RUNTIME_STATE"].MIN_TIME

    events = [
        (float(t.time_stage_in - base_time), file.size_mb)
        if t.time_stage_in else
        (float(t.time_stage_out - base_time), file.size_mb)
        for file in current_app.config["RUNTIME_STATE"].files.values() if file.producers
        for t in file.transfers if t.time_stage_in or t.time_stage_out
    ]

    if not events:
        return [], [0, 1], [0, 0], 'MB', 1.0, []

    df = pd.DataFrame(events, columns=["time", "delta"])
    df["time"] = df["time"].round(2)
    df = df.groupby("time", as_index=False)["delta"].sum()
    df["cumulative"] = df["delta"].cumsum()
    df["cumulative"] = df["cumulative"].clip(lower=0)

    if df.empty:
        return [], [0, 1], [0, 0], 'MB', 1.0, []

    raw_points = df[["time", "cumulative"]].values.tolist()

    x_max = df["time"].max()
    x_domain = [0, float(x_max) if pd.notna(x_max) else 1.0]

    y_max = df["cumulative"].max()
    unit, scale = get_unit_and_scale_by_max_file_size_mb(y_max if pd.notna(y_max) else 0)
    scaled_points = [[x, y * scale] for x, y in raw_points]
    y_domain = [0, y_max * scale if pd.notna(y_max) else 0]

    return scaled_points, x_domain, y_domain, unit, scale, raw_points


@file_transferred_size_bp.route('/file-transferred-size')
@check_and_reload_data()
def get_file_transferred_size():
    try:
        points, x_domain, y_domain, unit, _, _ = get_file_transferred_size_points()
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
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_file_transferred_size: {e}")
        return jsonify({'error': str(e)}), 500


@file_transferred_size_bp.route('/file-transferred-size/export-csv')
@check_and_reload_data()
def export_file_transferred_size_csv():
    try:
        _, _, _, unit, scale, raw_points = get_file_transferred_size_points()
        if not raw_points:
            return jsonify({'error': 'No file transferred data'}), 404

        sorted_points = sorted(raw_points, key=lambda x: x[0])
        
        def generate_csv():
            yield f"Time (s),Cumulative Size ({unit})\n"
            for t, cumulative in sorted_points:
                yield f"{t},{cumulative * scale}\n"

        return Response(
            generate_csv(),
            headers={
                "Content-Disposition": "attachment; filename=file_transferred_size.csv",
                "Content-Type": "text/csv"
            }
        )
    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in export_file_transferred_size_csv: {e}")
        return jsonify({'error': str(e)}), 500