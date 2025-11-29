# utils.py
import os
from datetime import datetime
import shutil

def timestamped_name(orig_name: str, marker: str = ".processed") -> str:
    """
    Return filename with timestamp inserted before extension.
    Example: data.csv -> data_20251122_235501.processed.csv
    """
    base, ext = os.path.splitext(orig_name)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base}_{ts}{marker}{ext}"

def safe_move(src: str, dst: str):
    """
    Move file (atomic if on same filesystem). Creates destination directory if needed.
    """
    dst_dir = os.path.dirname(dst)
    os.makedirs(dst_dir, exist_ok=True)
    # Use shutil.move (handles cross-filesystem)
    shutil.move(src, dst)
