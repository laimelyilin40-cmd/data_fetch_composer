"""
trades 處理器
保留 raw event store，提供可選的 resample 功能
"""

import zipfile
import io
import polars as pl
from typing import List, Optional
from datetime import datetime


class TradesProcessor:
    """trades 處理器"""
    
    def __init__(self):
        pass
    
    def process_file(self, zip_path: str) -> pl.DataFrame:
        """處理單一 zip 檔案（保留原始事件）"""
        with zipfile.ZipFile(zip_path, 'r') as zf:
            csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise ValueError(f"No CSV file found in {zip_path}")
            
            # 讀取所有 CSV
            dfs = []
            for csv_file in csv_files:
                content = zf.read(csv_file)
                df = pl.read_csv(io.BytesIO(content))
                dfs.append(df)
            
            if len(dfs) > 1:
                df = pl.concat(dfs)
            else:
                df = dfs[0]
        
        # 排序
        if "timestamp" in df.columns:
            df = df.sort("timestamp")
        
        return df
    
    def process_files(self, zip_paths: List[str]) -> pl.DataFrame:
        """處理多個檔案並合併"""
        dfs = [self.process_file(path) for path in zip_paths]
        return pl.concat(dfs).sort("timestamp")
    
    def resample_to_bars(self, df: pl.DataFrame, freq: str = "1m",
                        price_col: str = "price", qty_col: str = "quantity") -> pl.DataFrame:
        """
        將逐筆成交 resample 成 K 線
        
        Args:
            df: 原始 trades DataFrame
            freq: 頻率（例如 "1m", "5m", "1h"）
            price_col: 價格欄位名稱
            qty_col: 數量欄位名稱
        
        Returns:
            resampled DataFrame with columns: ts, open, high, low, close, volume, vwap, buy_volume, sell_volume
        """
        if "timestamp" not in df.columns:
            raise ValueError("DataFrame must have 'timestamp' column")
        
        # 轉換 timestamp 為 datetime（假設是毫秒）
        df = df.with_columns([
            (pl.col("timestamp") / 1000).cast(pl.Datetime).alias("dt")
        ])
        
        # 計算買賣方向（如果有 is_buyer_maker）
        if "is_buyer_maker" in df.columns:
            df = df.with_columns([
                (pl.when(pl.col("is_buyer_maker") == False)
                 .then(pl.col(qty_col))
                 .otherwise(0)
                 .alias("buy_qty")),
                (pl.when(pl.col("is_buyer_maker") == True)
                 .then(pl.col(qty_col))
                 .otherwise(0)
                 .alias("sell_qty"))
            ])
        else:
            df = df.with_columns([
                pl.lit(0).alias("buy_qty"),
                pl.lit(0).alias("sell_qty")
            ])
        
        # Group by 時間區間並聚合
        result = df.group_by_dynamic(
            "dt",
            every=freq,
            closed="left"
        ).agg([
            pl.col(price_col).first().alias("open"),
            pl.col(price_col).max().alias("high"),
            pl.col(price_col).min().alias("low"),
            pl.col(price_col).last().alias("close"),
            pl.col(qty_col).sum().alias("volume"),
            (pl.col(price_col) * pl.col(qty_col)).sum() / pl.col(qty_col).sum().alias("vwap"),
            pl.col("buy_qty").sum().alias("buy_volume"),
            pl.col("sell_qty").sum().alias("sell_volume"),
            pl.count().alias("trade_count")
        ])
        
        # 轉換回毫秒 timestamp
        result = result.with_columns([
            (pl.col("dt").cast(pl.Int64) * 1000).alias("ts")
        ]).drop("dt")
        
        return result

