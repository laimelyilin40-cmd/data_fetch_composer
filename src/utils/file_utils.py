"""
檔案工具函數
"""

import os
import hashlib
from pathlib import Path
from typing import Optional


def ensure_dir(path: str):
    """確保目錄存在"""
    Path(path).mkdir(parents=True, exist_ok=True)


def get_file_hash(file_path: str, algorithm: str = "sha256") -> Optional[str]:
    """計算檔案雜湊值"""
    if not os.path.exists(file_path):
        return None
    
    hash_obj = hashlib.new(algorithm)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def get_file_size(file_path: str) -> int:
    """取得檔案大小"""
    if os.path.exists(file_path):
        return os.path.getsize(file_path)
    return 0

