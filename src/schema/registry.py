"""
Schema Registry：儲存與查詢資料結構定義
"""

from datetime import datetime
from typing import List, Dict, Optional, Any
from sqlalchemy import (
    create_engine, Column, String, Integer, DateTime, Text, JSON, 
    ForeignKey, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import json

Base = declarative_base()


class Schema(Base):
    """Schema 定義表"""
    __tablename__ = "schemas"
    
    id = Column(Integer, primary_key=True)
    dataset_type = Column(String(50), unique=True, nullable=False, index=True)
    version = Column(String(20), default="1.0")
    
    columns = Column(JSON, nullable=False)  # List[{"name": str, "dtype": str, "position": int}]
    dtypes = Column(JSON, nullable=False)  # Dict[str, str]
    primary_time_key = Column(String(50), nullable=True)  # open_time, event_time, etc.
    join_key = Column(String(50), nullable=True)  # 建議的 join key
    field_notes = Column(JSON, nullable=True)  # Dict[str, str] - 欄位註解
    
    expected_schema = Column(JSON, nullable=True)  # 官方預期 schema（如果有）
    validation_status = Column(String(20), default="unknown")  # valid, invalid, unknown
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Sample(Base):
    """樣本檔案記錄表"""
    __tablename__ = "samples"
    
    id = Column(Integer, primary_key=True)
    dataset_type = Column(String(50), ForeignKey("schemas.dataset_type"), nullable=False, index=True)
    symbol = Column(String(20), nullable=False)
    sample_file_url = Column(Text, nullable=False)
    sample_date = Column(String(10), nullable=False)  # YYYY-MM-DD
    first_n_rows = Column(JSON, nullable=True)  # 前 N 行資料（避免太大）
    row_count = Column(Integer, nullable=True)
    file_size = Column(Integer, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)


class SchemaRegistry:
    """Schema Registry 管理器"""
    
    def __init__(self, db_path: str = "schema.db"):
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}", echo=False)
        self.Session = sessionmaker(bind=self.engine)
        self._init_db()
    
    def _init_db(self):
        """初始化資料庫"""
        Base.metadata.create_all(self.engine)
    
    def get_session(self) -> Session:
        """取得 session"""
        return self.Session()
    
    def register_schema(self, dataset_type: str, columns: List[Dict[str, Any]], 
                       dtypes: Dict[str, str], primary_time_key: Optional[str] = None,
                       join_key: Optional[str] = None, field_notes: Optional[Dict[str, str]] = None,
                       expected_schema: Optional[List[Dict[str, Any]]] = None,
                       validation_status: str = "unknown"):
        """註冊 schema"""
        with self.get_session() as session:
            schema = session.query(Schema).filter_by(dataset_type=dataset_type).first()
            
            if schema:
                # merge columns (monthly/daily 可能表頭不同；用 union 方式保留完整欄位集合)
                old_cols = schema.columns or []
                old_names = [c.get("name") for c in old_cols if c.get("name")]
                old_set = set(old_names)
                new_cols = columns or []
                merged_cols = list(old_cols)
                for c in new_cols:
                    nm = c.get("name")
                    if not nm:
                        continue
                    if nm not in old_set:
                        c2 = dict(c)
                        c2["position"] = len(merged_cols)
                        merged_cols.append(c2)
                        old_set.add(nm)

                schema.columns = merged_cols

                merged_dtypes = dict(schema.dtypes or {})
                merged_dtypes.update(dtypes or {})
                schema.dtypes = merged_dtypes

                schema.primary_time_key = primary_time_key or schema.primary_time_key
                schema.join_key = join_key or schema.join_key
                schema.field_notes = field_notes or {}
                schema.expected_schema = expected_schema
                schema.validation_status = validation_status
                schema.updated_at = datetime.utcnow()
            else:
                schema = Schema(
                    dataset_type=dataset_type,
                    columns=columns,
                    dtypes=dtypes,
                    primary_time_key=primary_time_key,
                    join_key=join_key,
                    field_notes=field_notes or {},
                    expected_schema=expected_schema,
                    validation_status=validation_status
                )
                session.add(schema)
            
            session.commit()
            return schema
    
    def get_schema(self, dataset_type: str) -> Optional[Dict[str, Any]]:
        """取得 schema"""
        with self.get_session() as session:
            schema = session.query(Schema).filter_by(dataset_type=dataset_type).first()
            if schema:
                return {
                    "dataset_type": schema.dataset_type,
                    "version": schema.version,
                    "columns": schema.columns,
                    "dtypes": schema.dtypes,
                    "primary_time_key": schema.primary_time_key,
                    "join_key": schema.join_key,
                    "field_notes": schema.field_notes,
                    "expected_schema": schema.expected_schema,
                    "validation_status": schema.validation_status
                }
            return None
    
    def add_sample(self, dataset_type: str, symbol: str, sample_file_url: str,
                  sample_date: str, first_n_rows: Optional[List[Dict]] = None,
                  row_count: Optional[int] = None, file_size: Optional[int] = None):
        """新增樣本記錄"""
        with self.get_session() as session:
            sample = Sample(
                dataset_type=dataset_type,
                symbol=symbol,
                sample_file_url=sample_file_url,
                sample_date=sample_date,
                first_n_rows=first_n_rows,
                row_count=row_count,
                file_size=file_size
            )
            session.add(sample)
            session.commit()
            return sample
    
    def get_samples(self, dataset_type: str) -> List[Dict[str, Any]]:
        """取得樣本列表"""
        with self.get_session() as session:
            samples = session.query(Sample).filter_by(dataset_type=dataset_type).all()
            return [
                {
                    "symbol": s.symbol,
                    "sample_file_url": s.sample_file_url,
                    "sample_date": s.sample_date,
                    "first_n_rows": s.first_n_rows,
                    "row_count": s.row_count,
                    "file_size": s.file_size
                }
                for s in samples
            ]

