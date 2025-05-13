from .runtime_state import runtime_state, LOGS_DIR

import os
from pathlib import Path
from flask import render_template, jsonify, request, Blueprint, Response

runtime_template_bp = Blueprint(
    'runtime_template', __name__, url_prefix='/api')


def all_subfolders_exists(parent: str, folder_names: list[str]) -> bool:
    parent_path = Path(parent).resolve()
    for folder_name in folder_names:
        target_path = parent_path / folder_name
        if not target_path.is_dir():
            return False
    return True

@runtime_template_bp.route('/runtime-template-list')
def get_runtime_template_list():
    log_folders = [name for name in os.listdir(LOGS_DIR)
                   if os.path.isdir(os.path.join(LOGS_DIR, name))]

    valid_runtime_templates = [
        name for name in sorted(log_folders)
        if all_subfolders_exists(os.path.join(LOGS_DIR, name), ['vine-logs', 'pkl-files'])
    ]

    return jsonify(valid_runtime_templates), 200

@runtime_template_bp.route('/change-runtime-template')
def change_runtime_template():
    runtime_template = request.args.get('runtime_template')
    success = runtime_state.change_runtime_template(runtime_template)
    return jsonify({'success': success}), 200


@runtime_template_bp.route('/logs/<runtime_template>')
def render_log_page(runtime_template):
    log_folders = [name for name in os.listdir(
        LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    log_folders_sorted = sorted(log_folders)
    if runtime_template != runtime_state.runtime_template:
        runtime_state.change_runtime_template(runtime_template)
    return render_template('index.html', log_folders=log_folders_sorted), 200
