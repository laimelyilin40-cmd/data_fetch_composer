"""
下載客戶端
"""

import requests
import os
from pathlib import Path
from typing import Optional
from tqdm import tqdm
import hashlib

from ..utils.file_utils import ensure_dir


class DownloadClient:
    """檔案下載客戶端"""
    
    def __init__(self, download_dir: str = "data/downloads"):
        self.download_dir = download_dir
        ensure_dir(download_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Binance-Vision-Downloader/1.0"
        })
    
    def download_file(self, url: str, local_path: Optional[str] = None,
                     verify_etag: Optional[str] = None,
                     expected_size: Optional[int] = None) -> str:
        """
        下載檔案
        
        Args:
            url: 遠端 URL
            local_path: 本地路徑（可選，會自動生成）
            verify_etag: 驗證 ETag（可選）
            expected_size: 預期大小（可選）
        
        Returns:
            本地檔案路徑
        """
        if local_path is None:
            # 從 URL 生成路徑
            filename = url.split("/")[-1]
            local_path = os.path.join(self.download_dir, filename)
        
        # 檢查是否已存在
        if os.path.exists(local_path):
            if verify_etag:
                # 可以檢查本地檔案的 ETag（需要額外儲存）
                pass
            if expected_size and os.path.getsize(local_path) == expected_size:
                return local_path
        
        # 下載
        ensure_dir(os.path.dirname(local_path))
        
        response = self.session.get(url, stream=True, timeout=60, allow_redirects=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get("Content-Length", 0))
        
        with open(local_path, "wb") as f, tqdm(
            desc=os.path.basename(local_path),
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024
        ) as pbar:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))
        
        return local_path

    def probe_file(self, url: str, timeout: int = 20) -> dict:
        """探測遠端檔案是否存在，並區分 404 與網路錯誤。"""
        try:
            response = self.session.head(url, timeout=timeout, allow_redirects=True)
            return {
                "exists": response.status_code == 200,
                "status_code": response.status_code,
                "size": int(response.headers.get("Content-Length", 0)) if response.status_code == 200 else None,
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "error": None,
                "error_type": None,
            }
        except requests.RequestException as e:
            return {
                "exists": False,
                "status_code": None,
                "size": None,
                "etag": None,
                "last_modified": None,
                "error": str(e),
                "error_type": "network",
            }
    
    def get_file_info(self, url: str) -> dict:
        """取得檔案資訊（不下載）"""
        return self.probe_file(url, timeout=10)

