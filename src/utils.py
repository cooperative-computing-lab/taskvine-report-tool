from pathlib import Path
from decimal import Decimal, ROUND_FLOOR

# check if all required subfolders exist
def all_subfolders_exists(parent: str, folder_names: list[str]) -> bool:
    parent_path = Path(parent).resolve()
    for folder_name in folder_names:
        target_path = parent_path / folder_name
        if not target_path.is_dir():
            return False
    return True

def floor_decimal(number, decimal_places):
    num = Decimal(str(number))
    quantizer = Decimal(f"1e-{decimal_places}")
    return float(num.quantize(quantizer, rounding=ROUND_FLOOR))