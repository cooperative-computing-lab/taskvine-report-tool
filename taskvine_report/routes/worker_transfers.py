from flask import Blueprint, jsonify, current_app
import pandas as pd
import os
from .utils import *

worker_transfers_bp = Blueprint('worker_transfers', __name__, url_prefix='/api')

@worker_transfers_bp.route('/worker-incoming-transfers')
@check_and_reload_data()
def get_worker_incoming_transfers():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_worker_incoming_transfers
        
        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404
        
        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({'error': 'CSV is empty'}), 404

        time_column = 'time (s)'
        time_values = df[time_column].values
        worker_columns = [col for col in df.columns if col != time_column]
        
        transfers = {}
        max_y = 0
        
        for worker_col in worker_columns:
            points = df[[time_column, worker_col]].dropna().values.tolist()
            transfers[worker_col] = points
            column_max = df[worker_col].max()
            if pd.notna(column_max):
                max_y = max(max_y, column_max)

        x_max = df[time_column].max() if not df.empty else 1.0
        x_domain = [0, float(x_max) if pd.notna(x_max) else 1.0]
        y_domain = [0, int(max_y)]

        return jsonify({
            'transfers': transfers,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_worker_incoming_transfers: {e}")
        return jsonify({'error': str(e)}), 500


@worker_transfers_bp.route('/worker-outgoing-transfers')
@check_and_reload_data()
def get_worker_outgoing_transfers():
    try:
        csv_path = current_app.config["RUNTIME_STATE"].csv_file_worker_outgoing_transfers
        if not os.path.exists(csv_path):
            return jsonify({'error': 'CSV file not found'}), 404

        df = pd.read_csv(csv_path)
        if df.empty:
            return jsonify({'error': 'CSV is empty'}), 404

        time_column = 'time (s)'
        worker_columns = [col for col in df.columns if col != time_column]

        transfers = {}
        max_y = 0

        for worker_col in worker_columns:
            points = df[[time_column, worker_col]].dropna().values.tolist()
            transfers[worker_col] = points
            column_max = df[worker_col].max()
            if pd.notna(column_max):
                max_y = max(max_y, column_max)

        x_max = df[time_column].max() if not df.empty else 1.0
        x_domain = [0, float(x_max) if pd.notna(x_max) else 1.0]
        y_domain = [0, int(max_y)]

        return jsonify({
            'transfers': transfers,
            'x_domain': x_domain,
            'y_domain': y_domain,
            'x_tick_values': compute_linear_tick_values(x_domain),
            'y_tick_values': compute_linear_tick_values(y_domain),
            'x_tick_formatter': d3_time_formatter(),
            'y_tick_formatter': d3_int_formatter(),
        })

    except Exception as e:
        current_app.config["RUNTIME_STATE"].log_error(f"Error in get_worker_outgoing_transfers: {e}")
        return jsonify({'error': str(e)}), 500
