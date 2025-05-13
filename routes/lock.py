from .runtime_state import runtime_state
from flask import Blueprint, Response, jsonify

lock_bp = Blueprint('lock', __name__)

@lock_bp.route('/api/lock', methods=['POST'])
def acquire_lock():
    if not runtime_state.template_lock.acquire():
        return Response(status=204)
    return jsonify({'message': 'Lock acquired'}), 200

@lock_bp.route('/api/unlock', methods=['POST'])
def release_lock():
    released = runtime_state.template_lock.release()
    if not released:
        return Response(status=204)
    return jsonify({'message': 'Lock released'}), 200
