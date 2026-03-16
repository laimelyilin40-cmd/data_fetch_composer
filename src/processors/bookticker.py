"""
bookTicker 專屬處理器
處理事件順序交錯、排序、去重
"""

import zipfile
import io
import polars as pl
from typing import List, Optional
from pathlib import Path


class BookTickerProcessor:
    """bookTicker 處理器"""
    
    CANONICAL_SCHEMA = {
        "ts": "int64",  # event_time
        "symbol": "str",
        "best_bid_price": "float64",
        "best_bid_qty": "float64",
        "best_ask_price": "float64",
        "best_ask_qty": "float64",
        "spread": "float64",  # 計算欄位
        "mid": "float64",  # 計算欄位
        "update_id": "int64",
        "transaction_time": "int64"
    }
    
    def __init__(self):
        pass
    
    def process_file(self, zip_path: str) -> pl.DataFrame:
        """
        處理單一 zip 檔案
        
        處理步驟：
        1. 解壓並讀取 CSV
        2. 排序（event_time, update_id）
        3. 去重
        4. 轉換為 canonical schema
        """
        with zipfile.ZipFile(zip_path, 'r') as zf:
            csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise ValueError(f"No CSV file found in {zip_path}")
            
            # 讀取所有 CSV（可能有多個）
            dfs = []
            for csv_file in csv_files:
                content = zf.read(csv_file)
                df = pl.read_csv(io.BytesIO(content))
                dfs.append(df)
            
            # 合併
            if len(dfs) > 1:
                df = pl.concat(dfs)
            else:
                df = dfs[0]
        
        # 標準化欄位名稱（根據已知 schema）
        # 預期欄位：update_id, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, transaction_time, event_time
        column_mapping = {
            "update_id": "update_id",
            "best_bid_price": "best_bid_price",
            "best_bid_qty": "best_bid_qty",
            "best_ask_price": "best_ask_price",
            "best_ask_qty": "best_ask_qty",
            "transaction_time": "transaction_time",
            "event_time": "event_time"
        }
        
        # 重新命名（如果需要的話）
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns and old_name != new_name:
                df = df.rename({old_name: new_name})
        
        # 排序：先按 event_time，再按 update_id
        if "event_time" in df.columns and "update_id" in df.columns:
            df = df.sort(["event_time", "update_id"])
        
        # 去重：基於 (event_time, update_id) 組合
        if "event_time" in df.columns and "update_id" in df.columns:
            df = df.unique(subset=["event_time", "update_id"], keep="first")
        
        # 計算衍生欄位
        if "best_bid_price" in df.columns and "best_ask_price" in df.columns:
            df = df.with_columns([
                (pl.col("best_ask_price") - pl.col("best_bid_price")).alias("spread"),
                ((pl.col("best_bid_price") + pl.col("best_ask_price")) / 2).alias("mid")
            ])
        
        # 重新命名 event_time 為 ts（canonical）
        if "event_time" in df.columns:
            df = df.rename({"event_time": "ts"})
        
        # 確保所有 canonical 欄位都存在
        for col, dtype in self.CANONICAL_SCHEMA.items():
            if col not in df.columns:
                if col == "symbol":
                    # 從檔案名推斷
                    symbol = self._extract_symbol(zip_path)
                    df = df.with_columns([pl.lit(symbol).alias("symbol")])
                elif col in ["spread", "mid"]:
                    # 已計算
                    pass
                else:
                    # 其他欄位設為 null
                    df = df.with_columns([pl.lit(None).alias(col)])
        
        return df
    
    def process_files(self, zip_paths: List[str]) -> pl.DataFrame:
        """處理多個檔案並合併"""
        dfs = [self.process_file(path) for path in zip_paths]
        return pl.concat(dfs).sort("ts")
    
    def _extract_symbol(self, zip_path: str) -> str:
        """從檔案路徑推斷 symbol"""
        filename = Path(zip_path).stem
        # 假設格式：BTCUSDT-2023-01-01
        parts = filename.split("-")
        return parts[0] if parts else "UNKNOWN"

