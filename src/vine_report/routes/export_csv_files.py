from src.vine_report.request_context import get_run_snapshot
import os
from flask import Blueprint, jsonify, send_file

def serve_existing_csv_file(csv_path, download_filename="data.csv"):
    if not os.path.exists(csv_path):
        return jsonify({'error': f'CSV file not found: {csv_path}'}), 404

    if os.path.getsize(csv_path) == 0:
        return jsonify({'error': 'CSV is empty'}), 404
    return send_file(
        csv_path,
        mimetype='text/csv',
        as_attachment=True,
        download_name=download_filename,
        conditional=True,
    )


def register_csv_export_routes(app):
    bp = Blueprint('csv_export', __name__, url_prefix='/api')

    @bp.route('/<path:slug>/export-csv')
    def export_csv(slug):
        try:
            filename = slug.replace('-', '_') + '.csv'
            csv_path = os.path.join(get_run_snapshot().csv_files_dir, filename)
            return serve_existing_csv_file(csv_path, filename)
        except Exception as e:
            get_run_snapshot().log_error(f"CSV export error for {slug}: {e}")
            return jsonify({'error': str(e)}), 500

    app.register_blueprint(bp)
