from pathlib import Path
import math

# check if all required subfolders exist
def all_subfolders_exists(parent: str, folder_names: list[str]) -> bool:
    parent_path = Path(parent).resolve()
    for folder_name in folder_names:
        target_path = parent_path / folder_name
        if not target_path.is_dir():
            return False
    return True

def floor_decimal(x, decimal_places):
    factor = 10 ** decimal_places
    return math.floor(x * factor) / factor

def get_unit_and_scale_by_max_file_size_mb(max_file_size_mb) -> tuple[str, float]:
    if max_file_size_mb >= 1024 * 1024:
        return 'TB', 1 / (1024 * 1024)
    elif max_file_size_mb >= 1024:
        return 'GB', 1 / 1024
    elif max_file_size_mb >= 1:
        return 'MB', 1
    elif max_file_size_mb >= 1 / 1024:
        return 'KB', 1024
    else:
        return 'Bytes', 1024 * 1024