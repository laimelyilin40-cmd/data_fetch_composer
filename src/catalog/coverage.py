"""
Coverage 分析器：提供資料覆蓋率分析
"""

from typing import List, Dict, Optional
from sqlalchemy.orm import Session

from .database import CatalogDB, File, Coverage


class CoverageAnalyzer:
    """Coverage 分析器"""
    
    def __init__(self, catalog_db: CatalogDB):
        self.catalog_db = catalog_db
    
    def get_coverage_matrix(self, symbols: List[str],
                           dataset_types: Optional[List[str]] = None,
                           market: Optional[str] = None) -> Dict:
        """
        取得 Coverage Matrix
        
        Returns:
            {
                "symbol": {
                    "dataset_type": {
                        "start_date": str,
                        "end_date": str,
                        "num_files": int,
                        "num_missing": int,
                        "missing_dates": List[str],
                        "total_size": int
                    }
                }
            }
        """
        with self.catalog_db.get_session() as session:
            result = {}
            
            for symbol in symbols:
                result[symbol] = {}
                
                # 查詢所有資料集類型
                query = session.query(File.dataset_type).filter_by(symbol=symbol).distinct()
                if market:
                    query = query.filter(File.market == market)
                if dataset_types:
                    query = query.filter(File.dataset_type.in_(dataset_types))
                
                ds_types = [row[0] for row in query.all()]
                
                for ds_type in ds_types:
                    # 查詢該資料集的所有檔案
                    q = session.query(File).filter_by(
                        symbol=symbol,
                        dataset_type=ds_type
                    )
                    if market:
                        q = q.filter(File.market == market)
                    files = q.all()
                    
                    if not files:
                        continue
                    
                    existing_files = [f for f in files if f.exists]
                    if not existing_files:
                        continue
                    
                    dates = sorted([f.date for f in existing_files])
                    missing_dates = [f.date for f in files if not f.exists]
                    
                    result[symbol][ds_type] = {
                        "start_date": dates[0],
                        "end_date": dates[-1],
                        "num_files": len(existing_files),
                        "num_missing": len(missing_dates),
                        "missing_dates": sorted(missing_dates),
                        "total_size": sum(f.remote_size or 0 for f in existing_files)
                    }
            
            return result
    
    def get_symbol_coverage(self, symbol: str, dataset_type: str,
                           interval: Optional[str] = None,
                           market: str = "futures_um") -> Optional[Dict]:
        """取得單一交易對的 coverage"""
        with self.catalog_db.get_session() as session:
            coverage = session.query(Coverage).filter_by(
                market=market,
                symbol=symbol,
                dataset_type=dataset_type,
                interval=interval
            ).first()
            
            if coverage:
                return {
                    "start_date": coverage.start_date,
                    "end_date": coverage.end_date,
                    "num_files": coverage.num_files,
                    "num_missing": coverage.num_missing,
                    "missing_dates": coverage.missing_date_list or [],
                    "total_size": coverage.total_size_estimate
                }
            return None
    
    def get_missing_dates(self, symbol: str, dataset_type: str,
                         start_date: str, end_date: str,
                         interval: Optional[str] = None,
                         market: str = "futures_um") -> List[str]:
        """取得缺失日期列表"""
        with self.catalog_db.get_session() as session:
            query = session.query(File).filter_by(
                market=market,
                symbol=symbol,
                dataset_type=dataset_type,
                interval=interval
            ).filter(
                File.date >= start_date,
                File.date <= end_date,
                File.exists == False
            )
            
            return [f.date for f in query.all()]

