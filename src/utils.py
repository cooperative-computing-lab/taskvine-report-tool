from pathlib import Path
import os


# check if all subfolders exist
def all_subfolders_exists(parent: str, folder_names: list[str]) -> bool:
    parent_path = Path(parent).resolve()
    for folder_name in folder_names:
        target_path = parent_path / folder_name
        if not target_path.is_dir():
            return False
    return True

# calculate the file size unit, the default is MB
def get_unit_and_scale_by_max_file_size_mb(max_file_size_mb) -> tuple[str, float]:
    if max_file_size_mb < 1 / 1024:
        return 'Bytes',  1024 * 1024
    elif max_file_size_mb < 1:
        return 'KB', 1024
    elif max_file_size_mb > 1024:
        return 'GB', 1 / 1024
    elif max_file_size_mb > 1024 * 1024:
        return 'TB', 1 / (1024 * 1024)
    else:
        return 'MB', 1

# get the file stat
def get_file_stat(file_path):
    try:
        stat = os.stat(file_path)
        return {
            'mtime': stat.st_mtime,
            'size': stat.st_size
        }
    except Exception as e:
        return None
    
def build_request_info_string(request):
    method = request.method
    path = request.path
    args = dict(request.args)
    headers = {k: v for k, v in request.headers if k not in ['Cookie', 'Authorization']}
    remote_addr = request.remote_addr

    request_info = {
        'method': method,
        'path': path,
        'args': args,
        'headers': headers,
        'remote_addr': remote_addr
    }

    if path.startswith('/api/'):
        return f"API Request: {method} {path} - {request_info}"
    else:
        return f"HTTP Request: {method} {path}"

def build_response_info_string(response, request, duration=None):
    path = request.path
    status_code = response.status_code

    if path.startswith('/api/'):
        if duration:
            return f"API Response: {status_code} for {path} - completed in {duration:.4f}s"
        else:
            return f"API Response: {status_code} for {path}"
    elif status_code >= 400:
        return f"HTTP Error Response: {status_code} for {path}"
    else:
        return f"HTTP Response: {status_code} for {path}"
