import os
from flask import Blueprint, current_app, jsonify, make_response
from io import StringIO
import pandas as pd

def serve_existing_csv_file(csv_path, download_filename="data.csv"):
    if not os.path.exists(csv_path):
        return jsonify({'error': f'CSV file not found: {csv_path}'}), 404

    df = pd.read_csv(csv_path)
    if df.empty:
        return jsonify({'error': 'CSV is empty'}), 404

    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    response = make_response(buffer.getvalue())
    response.headers['Content-Disposition'] = f'attachment; filename={download_filename}'
    response.headers['Content-Type'] = 'text/csv'
    return response


def register_csv_export_routes(app):
    bp = Blueprint('csv_export', __name__, url_prefix='/api')

    @bp.route('/<path:slug>/export-csv')
    def export_csv(slug):
        try:
            filename = slug.replace('-', '_') + '.csv'
            csv_path = os.path.join(current_app.config["RUNTIME_STATE"].csv_files_dir, filename)
            return serve_existing_csv_file(csv_path, filename)
        except Exception as e:
            current_app.config["RUNTIME_STATE"].log_error(f"CSV export error for {slug}: {e}")
            return jsonify({'error': str(e)}), 500

    app.register_blueprint(bp)
