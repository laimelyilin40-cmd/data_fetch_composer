"""
下載管理器
"""

import os
from typing import List, Optional
from datetime import datetime
from .client import DownloadClient
from ..catalog.database import CatalogDB, File


class DownloadManager:
    """下載管理器"""
    
    def __init__(self, catalog_db: CatalogDB, download_dir: str = "data/downloads"):
        self.catalog_db = catalog_db
        self.client = DownloadClient(download_dir)
    
    def download_files(self, dataset_type: str, symbol: str,
                      start_date: str, end_date: str,
                      interval: Optional[str] = None,
                      cadence: str = "daily") -> List[str]:
        """
        下載指定範圍的檔案
        
        Returns:
            下載的本地檔案路徑列表
        """
        with self.catalog_db.get_session() as session:
            query = session.query(File).filter_by(
                dataset_type=dataset_type,
                symbol=symbol,
                interval=interval,
                cadence=cadence
            ).filter(
                File.date >= start_date,
                File.date <= end_date,
                File.exists == True
            )
            
            files = query.all()
            downloaded_paths = []
            
            for file in files:
                try:
                    local_path = self.client.download_file(
                        file.remote_url,
                        verify_etag=file.etag,
                        expected_size=file.remote_size
                    )
                    
                    # 更新資料庫
                    file.local_path = local_path
                    file.local_size = os.path.getsize(local_path)
                    file.downloaded_at = datetime.utcnow()
                    session.commit()
                    
                    downloaded_paths.append(local_path)
                except Exception as e:
                    print(f"Failed to download {file.remote_url}: {e}")
            
            return downloaded_paths

