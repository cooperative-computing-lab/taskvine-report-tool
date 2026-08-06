from flask import Blueprint, Response, jsonify, current_app


lock_bp = Blueprint('lock', __name__)


@lock_bp.route('/api/lock', methods=['POST'])
def acquire_lock():
    if not current_app.config["RUNTIME_STATE"].template_lock.acquire():
        return Response(status=204)
    return jsonify({'message': 'Lock acquired'}), 200

@lock_bp.route('/api/unlock', methods=['POST'])
def release_lock():
    released = current_app.config["RUNTIME_STATE"].template_lock.release()
    if not released:
        return Response(status=204)
    return jsonify({'message': 'Lock released'}), 200
