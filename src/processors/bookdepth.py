"""
bookDepth 處理器
先當成衍生指標處理，不做 L2 回放假設
"""

import zipfile
import io
import polars as pl
from typing import List, Optional, Tuple


class BookDepthProcessor:
    """bookDepth 處理器"""
    
    def __init__(self):
        self.semantics_note = "Derived depth metrics - not raw L2 orderbook"
    
    def process_file(self, zip_path: str) -> Tuple[pl.DataFrame, dict]:
        """
        處理單一 zip 檔案
        
        Returns:
            (df, metadata) - 資料框與 metadata（包含語義警告）
        """
        metadata = {
            "semantics": "derived",
            "warning": "This dataset may not represent raw L2 orderbook. Use with caution.",
            "columns_found": []
        }
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise ValueError(f"No CSV file found in {zip_path}")
            
            # 讀取第一個 CSV（通常只有一個）
            content = zf.read(csv_files[0])
            df = pl.read_csv(io.BytesIO(content))
            
            metadata["columns_found"] = df.columns
        
        # 檢查是否有已知的衍生欄位（根據 issue #447）
        known_derived_cols = ["percentage", "depth", "notional"]
        found_derived = [col for col in df.columns if any(dc in col.lower() for dc in known_derived_cols)]
        if found_derived:
            metadata["derived_columns"] = found_derived
            metadata["warning"] += f" Found derived columns: {found_derived}"
        
        # 排序（如果有時間欄位）
        time_cols = [col for col in df.columns if "time" in col.lower() or "timestamp" in col.lower()]
        if time_cols:
            df = df.sort(time_cols[0])
        
        return df, metadata
    
    def process_files(self, zip_paths: List[str]) -> Tuple[pl.DataFrame, List[dict]]:
        """處理多個檔案並合併"""
        all_dfs = []
        all_metadata = []
        
        for zip_path in zip_paths:
            df, metadata = self.process_file(zip_path)
            all_dfs.append(df)
            all_metadata.append(metadata)
        
        # 合併
        combined_df = pl.concat(all_dfs)
        
        # 排序
        time_cols = [col for col in combined_df.columns if "time" in col.lower() or "timestamp" in col.lower()]
        if time_cols:
            combined_df = combined_df.sort(time_cols[0])
        
        return combined_df, all_metadata

