"""
aggTrades 專屬處理器
處理 zip 內巢狀目錄、同名 CSV 多份、去重
"""

import zipfile
import io
import polars as pl
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import hashlib


class AggTradesProcessor:
    """aggTrades 處理器"""
    
    def __init__(self):
        pass
    
    def process_file(self, zip_path: str) -> Tuple[pl.DataFrame, Dict]:
        """
        處理單一 zip 檔案
        
        Returns:
            (df, manifest) - 資料框與 manifest 資訊
        """
        manifest = {
            "zip_path": zip_path,
            "members": [],
            "issues": []
        }
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # 列出所有成員
            all_members = zf.namelist()
            csv_members = [m for m in all_members if m.endswith('.csv')]
            
            # 檢查是否有巢狀目錄
            has_nested = any('/' in m or '\\' in m for m in csv_members)
            if has_nested:
                manifest["issues"].append("Nested directory structure detected")
            
            # 檢查是否有同名檔案
            member_names = [Path(m).name for m in csv_members]
            if len(member_names) != len(set(member_names)):
                manifest["issues"].append("Duplicate CSV filenames detected")
            
            # 讀取所有 CSV
            dfs = []
            for csv_member in csv_members:
                try:
                    content = zf.read(csv_member)
                    df = pl.read_csv(io.BytesIO(content))
                    
                    member_info = {
                        "path": csv_member,
                        "size": len(content),
                        "rows": len(df),
                        "hash": hashlib.md5(content).hexdigest()[:8]
                    }
                    manifest["members"].append(member_info)
                    
                    dfs.append(df)
                except Exception as e:
                    manifest["issues"].append(f"Failed to read {csv_member}: {e}")
            
            if not dfs:
                raise ValueError(f"No valid CSV found in {zip_path}")
            
            # 合併所有 DataFrame
            if len(dfs) > 1:
                df = pl.concat(dfs)
            else:
                df = dfs[0]
            
            # 去重：基於 (timestamp, price, quantity, first_trade_id, last_trade_id)
            # 或使用 agg_trade_id（如果存在）
            if "agg_trade_id" in df.columns:
                df = df.unique(subset=["agg_trade_id"], keep="first")
            elif all(col in df.columns for col in ["timestamp", "price", "quantity", "first_trade_id", "last_trade_id"]):
                df = df.unique(
                    subset=["timestamp", "price", "quantity", "first_trade_id", "last_trade_id"],
                    keep="first"
                )
            else:
                # 如果沒有明確的 key，至少按 timestamp 排序
                if "timestamp" in df.columns:
                    df = df.sort("timestamp")
                    manifest["issues"].append("No clear deduplication key, sorted by timestamp only")
        
        return df, manifest
    
    def process_files(self, zip_paths: List[str]) -> Tuple[pl.DataFrame, List[Dict]]:
        """處理多個檔案並合併"""
        all_dfs = []
        all_manifests = []
        
        for zip_path in zip_paths:
            df, manifest = self.process_file(zip_path)
            all_dfs.append(df)
            all_manifests.append(manifest)
        
        # 合併所有資料
        combined_df = pl.concat(all_dfs)
        
        # 最終去重（跨檔案）
        if "agg_trade_id" in combined_df.columns:
            combined_df = combined_df.unique(subset=["agg_trade_id"], keep="first")
        
        # 排序
        if "timestamp" in combined_df.columns:
            combined_df = combined_df.sort("timestamp")
        
        return combined_df, all_manifests

