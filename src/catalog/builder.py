"""
Catalog Builder：從遠端盤點並建立資料目錄
"""

import requests
from datetime import datetime
from typing import List, Optional, Dict
from urllib.parse import urljoin
from tqdm import tqdm

from .database import CatalogDB
from ..utils.time_utils import generate_date_list, parse_date_range


class CatalogBuilder:
    """資料目錄建置器"""

    BASE_URLS = {
        # futures UM (USDⓈ-M)
        "futures_um": "https://data.binance.vision/data/futures/um",
        # spot
        "spot": "https://data.binance.vision/data/spot",
    }
    
    # 已知的資料集類型
    DATASET_TYPES = [
        "aggTrades", "bookDepth", "bookTicker", "indexPriceKlines",
        "klines", "markPriceKlines", "metrics", "premiumIndexKlines", "fundingRate", "trades"
    ]
    
    # 需要 interval 的資料集
    INTERVAL_DATASETS = ["klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"]
    
    # 常見的 interval
    INTERVALS = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]
    
    def __init__(self, catalog_db: CatalogDB, market: str = "futures_um"):
        self.catalog_db = catalog_db
        if market not in self.BASE_URLS:
            raise ValueError(f"Unsupported market: {market}. Supported: {list(self.BASE_URLS)}")
        self.market = market
        self.base_url = self.BASE_URLS[market]
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Binance-Vision-Catalog/1.0"
        })
    
    def build_catalog(self, symbols: List[str], start_date: str, end_date: str,
                     dataset_types: Optional[List[str]] = None,
                     intervals: Optional[List[str]] = None,
                     cadence: str = "daily"):
        """
        建立資料目錄
        
        Args:
            symbols: 交易對列表
            start_date: 開始日期 (YYYY-MM-DD)
            end_date: 結束日期 (YYYY-MM-DD)
            dataset_types: 資料集類型列表，None 則使用全部
            intervals: interval 列表（僅用於需要 interval 的資料集）
            cadence: daily 或 monthly
        """
        if dataset_types is None:
            dataset_types = self.DATASET_TYPES
        
        if intervals is None:
            intervals = self.INTERVALS
        
        # 註冊所有資料集類型
        for ds_type in dataset_types:
            self.catalog_db.register_dataset(
                dataset_type=ds_type,
                market=self.market,
                cadence=cadence,
                requires_interval=ds_type in self.INTERVAL_DATASETS,
                is_event_stream=ds_type in ["trades", "bookTicker", "aggTrades"]
            )
        
        # 註冊所有交易對
        for symbol in symbols:
            self.catalog_db.register_symbol(symbol, market=self.market)
        
        # 產生日期列表
        start_dt, end_dt = parse_date_range(start_date, end_date)
        dates = generate_date_list(start_dt, end_dt, cadence)
        
        # 盤點每個組合
        total_tasks = len(symbols) * len(dataset_types) * len(dates)
        if any(ds in dataset_types for ds in self.INTERVAL_DATASETS):
            total_tasks *= len(intervals)
        
        with tqdm(total=total_tasks, desc="Building catalog") as pbar:
            for symbol in symbols:
                for ds_type in dataset_types:
                    if ds_type in self.INTERVAL_DATASETS:
                        for interval in intervals:
                            for date in dates:
                                self._check_file(symbol, ds_type, date, cadence, interval)
                                pbar.update(1)
                    else:
                        for date in dates:
                            self._check_file(symbol, ds_type, date, cadence, None)
                            pbar.update(1)
    
    def _check_file(self, symbol: str, dataset_type: str, date: str, 
                   cadence: str, interval: Optional[str] = None):
        """檢查單一檔案是否存在"""
        # 構建 URL
        url = self._build_url(symbol, dataset_type, date, cadence, interval)
        
        # 嘗試 HEAD 請求
        try:
            response = self.session.head(url, timeout=10, allow_redirects=True)
            exists = response.status_code == 200
            remote_size = int(response.headers.get("Content-Length", 0)) if exists else None
            
            # 解析 Last-Modified
            last_modified = None
            if "Last-Modified" in response.headers:
                try:
                    last_modified = datetime.strptime(
                        response.headers["Last-Modified"],
                        "%a, %d %b %Y %H:%M:%S %Z"
                    )
                except:
                    pass
            
            etag = response.headers.get("ETag")
            
        except Exception as e:
            exists = False
            remote_size = None
            last_modified = None
            etag = None
        
        # 更新資料庫
        self.catalog_db.upsert_file(
            dataset_type=dataset_type,
            symbol=symbol,
            date=date,
            cadence=cadence,
            interval=interval,
            remote_url=url,
            exists=exists,
            remote_size=remote_size,
            last_modified=last_modified,
            etag=etag,
            market=self.market
        )
    
    def _build_url(self, symbol: str, dataset_type: str, date: str,
                  cadence: str, interval: Optional[str] = None) -> str:
        """構建檔案 URL"""
        # Vision 的目錄結構：
        # - interval datasets (klines / markPriceKlines / indexPriceKlines / premiumIndexKlines):
        #   {base}/{cadence}/{dataset_type}/{symbol}/{interval}/{symbol}-{interval}-{date}.zip
        # - non-interval datasets (trades / aggTrades / metrics / bookDepth / bookTicker ...):
        #   {base}/{cadence}/{dataset_type}/{symbol}/{symbol}-{dataset_type}-{date}.zip

        if cadence not in ("daily", "monthly"):
            raise ValueError(f"Unsupported cadence: {cadence}")

        if interval:
            filename = f"{symbol}-{interval}-{date}.zip"
            path = f"{cadence}/{dataset_type}/{symbol}/{interval}/{filename}"
        else:
            filename = f"{symbol}-{dataset_type}-{date}.zip"
            path = f"{cadence}/{dataset_type}/{symbol}/{filename}"

        return urljoin(self.base_url + "/", path)
    
    def update_coverage(self, dataset_type: str, symbol: str, interval: Optional[str] = None, cadence: str = "daily"):
        """更新 coverage 摘要"""
        with self.catalog_db.get_session() as session:
            from .database import File, Coverage
            
            # 查詢所有檔案
            query = session.query(File).filter_by(
                market=self.market,
                dataset_type=dataset_type,
                symbol=symbol,
                interval=interval,
                cadence=cadence
            )
            
            files = query.all()
            if not files:
                return
            
            # 計算日期範圍
            dates = sorted([f.date for f in files if f.exists])
            if not dates:
                return
            
            start_date = dates[0]
            end_date = dates[-1]
            
            # 計算缺失日期（簡化版：假設連續日期）
            # 實際應該根據 cadence 和 interval 計算預期日期列表
            num_files = len([f for f in files if f.exists])
            num_missing = len([f for f in files if not f.exists])
            missing_dates = [f.date for f in files if not f.exists]
            
            total_size = sum(f.remote_size or 0 for f in files if f.exists)
            
            # 更新或建立 coverage
            coverage = session.query(Coverage).filter_by(
                market=self.market,
                dataset_type=dataset_type,
                symbol=symbol,
                interval=interval,
                cadence=cadence
            ).first()
            
            if coverage:
                coverage.start_date = start_date
                coverage.end_date = end_date
                coverage.num_files = num_files
                coverage.num_missing = num_missing
                coverage.missing_date_list = missing_dates
                coverage.total_size_estimate = total_size
                coverage.last_updated = datetime.utcnow()
            else:
                coverage = Coverage(
                    market=self.market,
                    dataset_type=dataset_type,
                    symbol=symbol,
                    interval=interval,
                    cadence=cadence,
                    start_date=start_date,
                    end_date=end_date,
                    num_files=num_files,
                    num_missing=num_missing,
                    missing_date_list=missing_dates,
                    total_size_estimate=total_size
                )
                session.add(coverage)
            
            session.commit()

