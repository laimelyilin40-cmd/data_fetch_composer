"""
資料合併器
"""

import polars as pl
from typing import List, Dict, Optional
from pathlib import Path
from datetime import datetime

from .recipe import Recipe, InputSource
from ..catalog.database import CatalogDB
from ..schema.registry import SchemaRegistry
from ..downloader.manager import DownloadManager
from ..processors import (
    BookTickerProcessor, AggTradesProcessor, 
    TradesProcessor, BookDepthProcessor
)


class DatasetMerger:
    """資料合併器"""
    
    PROCESSORS = {
        "bookTicker": BookTickerProcessor,
        "aggTrades": AggTradesProcessor,
        "trades": TradesProcessor,
        "bookDepth": BookDepthProcessor
    }
    
    def __init__(self, catalog_db: CatalogDB, schema_registry: SchemaRegistry,
                 download_dir: str = "data/downloads", output_dir: str = "data/outputs"):
        self.catalog_db = catalog_db
        self.schema_registry = schema_registry
        self.download_manager = DownloadManager(catalog_db, download_dir)
        self.output_dir = output_dir
        Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    def execute_recipe(self, recipe: Recipe) -> str:
        """
        執行 recipe 並產生輸出
        
        Returns:
            輸出檔案路徑
        """
        # 1. 下載資料
        downloaded_files = self._download_data(recipe)
        
        # 2. 處理並載入資料
        dataframes = {}
        for input_source in recipe.inputs:
            key = f"{input_source.dataset_type}_{input_source.interval or 'none'}"
            df = self._load_and_process(
                input_source,
                downloaded_files.get(key, [])
            )
            dataframes[key] = df
        
        # 3. 合併資料
        merged_df = self._merge_dataframes(dataframes, recipe)
        
        # 4. 格式化輸出
        formatted_df = self._format_output(merged_df, recipe)
        
        # 5. 儲存
        output_path = self._save_output(formatted_df, recipe)
        
        # 6. 產生 manifest
        self._create_manifest(recipe, downloaded_files, output_path)
        
        return output_path
    
    def _download_data(self, recipe: Recipe) -> Dict[str, List[str]]:
        """下載所需資料"""
        downloaded = {}
        
        for input_source in recipe.inputs:
            key = f"{input_source.dataset_type}_{input_source.interval or 'none'}"
            downloaded[key] = []
            
            for symbol in recipe.symbols:
                paths = self.download_manager.download_files(
                    dataset_type=input_source.dataset_type,
                    symbol=symbol,
                    start_date=recipe.time_range["start"],
                    end_date=recipe.time_range["end"],
                    interval=input_source.interval,
                    cadence="daily"
                )
                downloaded[key].extend(paths)
        
        return downloaded
    
    def _load_and_process(self, input_source: InputSource, file_paths: List[str]) -> pl.DataFrame:
        """載入並處理資料"""
        if not file_paths:
            raise ValueError(f"No files to process for {input_source.dataset_type}")
        
        # 使用專屬處理器（如果有）
        if input_source.dataset_type in self.PROCESSORS:
            processor = self.PROCESSORS[input_source.dataset_type]()
            
            if input_source.dataset_type == "bookTicker":
                df = processor.process_files(file_paths)
            elif input_source.dataset_type == "aggTrades":
                df, _ = processor.process_files(file_paths)
            elif input_source.dataset_type == "trades":
                df = processor.process_files(file_paths)
                if input_source.resample:
                    df = processor.resample_to_bars(
                        df, 
                        freq=input_source.resample_freq or "1m"
                    )
            elif input_source.dataset_type == "bookDepth":
                df, _ = processor.process_files(file_paths)
            else:
                df = self._load_generic(file_paths)
        else:
            df = self._load_generic(file_paths)
        
        # 選取指定欄位
        available_cols = df.columns
        requested_cols = [col for col in input_source.columns if col in available_cols]
        
        if not requested_cols:
            raise ValueError(f"None of requested columns {input_source.columns} found in {input_source.dataset_type}")
        
        df = df.select(requested_cols)
        
        return df
    
    def _load_generic(self, file_paths: List[str]) -> pl.DataFrame:
        """通用載入（用於 klines, metrics 等）"""
        import zipfile
        import io
        
        dfs = []
        for zip_path in file_paths:
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                    for csv_file in csv_files:
                        content = zf.read(csv_file)
                        df = pl.read_csv(io.BytesIO(content))
                        dfs.append(df)
            except Exception as e:
                print(f"Warning: Failed to load {zip_path}: {e}")
        
        if not dfs:
            raise ValueError("No data loaded")
        
        return pl.concat(dfs)
    
    def _merge_dataframes(self, dataframes: Dict[str, pl.DataFrame], recipe: Recipe) -> pl.DataFrame:
        """合併多個 DataFrame"""
        join_key = recipe.join_policy["key"]
        missing_policy = recipe.join_policy.get("missing", "drop")
        
        # 從第一個 DataFrame 開始
        result = None
        
        for key, df in dataframes.items():
            if result is None:
                result = df
            else:
                # 確保 join_key 存在
                if join_key not in df.columns:
                    # 嘗試使用 primary_time_key
                    schema = self.schema_registry.get_schema(key.split("_")[0])
                    if schema and schema.get("primary_time_key"):
                        time_key = schema["primary_time_key"]
                        if time_key in df.columns:
                            df = df.rename({time_key: join_key})
                        else:
                            raise ValueError(f"Cannot find join key '{join_key}' in {key}")
                    else:
                        raise ValueError(f"Cannot find join key '{join_key}' in {key}")
                
                # 合併
                if missing_policy == "drop":
                    result = result.join(df, on=join_key, how="inner")
                elif missing_policy == "ffill":
                    result = result.join(df, on=join_key, how="left")
                    # TODO: 實作 forward fill
                else:  # keep_nan
                    result = result.join(df, on=join_key, how="left")
        
        return result
    
    def _format_output(self, df: pl.DataFrame, recipe: Recipe) -> pl.DataFrame:
        """格式化輸出（long/wide）"""
        if recipe.output_format == "long":
            # long format: ts, symbol, features...
            # 需要從資料中提取 symbol（如果沒有）
            if "symbol" not in df.columns:
                # 可以從其他欄位推斷或添加
                pass
            return df
        else:  # wide
            # wide format: ts, BTC_close, ETH_close...
            # 需要 pivot（假設有 symbol 欄位）
            # 簡化版：直接返回（實際需要更複雜的轉換）
            return df
    
    def _save_output(self, df: pl.DataFrame, recipe: Recipe) -> str:
        """儲存輸出"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{recipe.name}_{timestamp}.parquet"
        output_path = Path(self.output_dir) / filename
        
        df.write_parquet(output_path)
        return str(output_path)
    
    def _create_manifest(self, recipe: Recipe, downloaded_files: Dict, output_path: str):
        """產生 manifest"""
        manifest = {
            "recipe": recipe.to_dict(),
            "output_path": output_path,
            "generated_at": datetime.utcnow().isoformat(),
            "source_files": downloaded_files,
            "row_count": None,  # 可以從 output 讀取
            "columns": None
        }
        
        manifest_path = Path(output_path).with_suffix(".manifest.json")
        import json
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

