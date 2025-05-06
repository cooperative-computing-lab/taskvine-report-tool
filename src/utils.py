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