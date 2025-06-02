import os
from pathlib import Path
from flask import jsonify, request, Blueprint, current_app

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
    log_folders = [name for name in os.listdir(current_app.config["RUNTIME_STATE"].logs_dir)
                   if os.path.isdir(os.path.join(current_app.config["RUNTIME_STATE"].logs_dir, name))]

    valid_runtime_templates = [
        name for name in sorted(log_folders)
        if all_subfolders_exists(os.path.join(current_app.config["RUNTIME_STATE"].logs_dir, name), ['vine-logs', 'pkl-files'])
    ]

    return jsonify(valid_runtime_templates), 200

@runtime_template_bp.route('/change-runtime-template')
def change_runtime_template():
    runtime_template = request.args.get('runtime_template')
    if runtime_template and runtime_template != current_app.config["RUNTIME_STATE"].runtime_template:
        success = current_app.config["RUNTIME_STATE"].reload_template(runtime_template)
    else:
        success = True
    return jsonify({'success': success}), 200


@runtime_template_bp.route('/reload-runtime-template')
def reload_runtime_template():
    runtime_template = request.args.get('runtime_template')
    if runtime_template:
        success = current_app.config["RUNTIME_STATE"].reload_template(runtime_template)
    else:
        success = False
    return jsonify({'success': success}), 200
