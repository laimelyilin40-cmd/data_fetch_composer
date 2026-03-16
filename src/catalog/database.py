"""
Catalog 資料庫 Schema 與 ORM
"""

from datetime import datetime
from typing import Optional, List
from sqlalchemy import (
    create_engine, Column, String, Integer, Boolean, DateTime, 
    Float, Text, JSON, ForeignKey, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.exc import OperationalError
import json

Base = declarative_base()


class Dataset(Base):
    """資料集類型定義表"""
    __tablename__ = "datasets"
    
    id = Column(Integer, primary_key=True)
    dataset_type = Column(String(50), unique=True, nullable=False, index=True)
    market = Column(String(20), default="um")  # um, cm
    cadence = Column(String(20))  # daily, monthly
    requires_interval = Column(Boolean, default=False)  # klines 需要 interval
    is_event_stream = Column(Boolean, default=False)  # trades, bookTicker 等
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    files = relationship("File", back_populates="dataset")


class Symbol(Base):
    """交易對表"""
    __tablename__ = "symbols"
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), unique=True, nullable=False, index=True)
    market = Column(String(20), default="um")
    listed_at = Column(DateTime, nullable=True)
    status = Column(String(20), default="TRADING")  # TRADING, BREAK, etc.
    created_at = Column(DateTime, default=datetime.utcnow)
    
    files = relationship("File", back_populates="symbol_obj")


class File(Base):
    """檔案索引表（核心表）"""
    __tablename__ = "files"
    
    id = Column(Integer, primary_key=True)
    market = Column(String(20), default="futures_um", index=True)  # futures_um / spot / futures_cm
    dataset_type = Column(String(50), ForeignKey("datasets.dataset_type"), nullable=False, index=True)
    symbol = Column(String(20), ForeignKey("symbols.symbol"), nullable=False, index=True)
    interval = Column(String(10), nullable=True, index=True)  # 1m, 5m, 1h, etc.
    date = Column(String(10), nullable=False, index=True)  # YYYY-MM-DD or YYYY-MM
    cadence = Column(String(20), nullable=False)  # daily, monthly
    
    remote_url = Column(Text, nullable=False)
    exists = Column(Boolean, default=False)
    remote_size = Column(Integer, nullable=True)  # bytes
    last_modified = Column(DateTime, nullable=True)
    etag = Column(String(100), nullable=True)
    
    local_path = Column(Text, nullable=True)  # 下載後的路徑
    local_size = Column(Integer, nullable=True)
    downloaded_at = Column(DateTime, nullable=True)
    
    notes = Column(Text, nullable=True)  # 資料品質問題、異常等
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    dataset = relationship("Dataset", back_populates="files")
    symbol_obj = relationship("Symbol", back_populates="files")
    
    __table_args__ = (
        Index("idx_file_lookup", "market", "dataset_type", "symbol", "interval", "date", "cadence"),
    )


class Coverage(Base):
    """Coverage 摘要表"""
    __tablename__ = "coverage"
    
    id = Column(Integer, primary_key=True)
    market = Column(String(20), default="futures_um", index=True)
    dataset_type = Column(String(50), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    interval = Column(String(10), nullable=True, index=True)
    cadence = Column(String(20), default="daily", index=True)  # daily / monthly
    
    start_date = Column(String(10), nullable=False)  # YYYY-MM-DD
    end_date = Column(String(10), nullable=False)
    num_files = Column(Integer, default=0)
    num_missing = Column(Integer, default=0)
    missing_date_list = Column(JSON, nullable=True)  # List[str]
    total_size_estimate = Column(Integer, nullable=True)  # bytes
    
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index("idx_coverage_lookup", "market", "dataset_type", "symbol", "interval", "cadence"),
    )


class CatalogDB:
    """Catalog 資料庫管理器"""
    
    def __init__(self, db_path: str = "catalog.db"):
        self.db_path = db_path
        self.engine = create_engine(
            f"sqlite:///{db_path}",
            echo=False,
            connect_args={
                # 在 Streamlit 多執行緒/多連線時，給 SQLite 足夠等待時間避免立刻報 locked
                "timeout": 30,
                "check_same_thread": False,
            },
        )
        self.Session = sessionmaker(bind=self.engine)
    
    def init_database(self):
        """初始化資料庫表結構"""
        Base.metadata.create_all(self.engine)
        self._apply_sqlite_pragmas()
        self._migrate_if_needed()

    def _apply_sqlite_pragmas(self):
        """提升 SQLite 並行穩定性；失敗時不阻斷流程。"""
        try:
            with self.engine.connect() as conn:
                conn.exec_driver_sql("PRAGMA busy_timeout=30000")
                conn.exec_driver_sql("PRAGMA journal_mode=WAL")
                conn.exec_driver_sql("PRAGMA synchronous=NORMAL")
        except Exception:
            pass

    def _exec_sql_ignore_locked(self, conn, sql: str):
        try:
            conn.exec_driver_sql(sql)
        except OperationalError as e:
            if "database is locked" in str(e).lower():
                # 其他程序正在寫入時，先跳過 migration backfill，避免 UI 直接中斷
                return
            raise

    def _migrate_if_needed(self):
        """
        輕量 migration：
        - 為既有 catalog.db 補上 market 欄位（files/coverage）
        """
        with self.engine.begin() as conn:
            # files.market
            cols = [row[1] for row in conn.exec_driver_sql("PRAGMA table_info(files)").fetchall()]
            if "market" not in cols:
                self._exec_sql_ignore_locked(conn, "ALTER TABLE files ADD COLUMN market VARCHAR(20) DEFAULT 'futures_um'")
                self._exec_sql_ignore_locked(conn, "CREATE INDEX IF NOT EXISTS ix_files_market ON files (market)")
            # backfill (SQLite 既有 rows 可能是 NULL)
            self._exec_sql_ignore_locked(conn, "UPDATE files SET market='futures_um' WHERE market IS NULL OR market=''")

            # coverage.market
            cols = [row[1] for row in conn.exec_driver_sql("PRAGMA table_info(coverage)").fetchall()]
            if "market" not in cols:
                self._exec_sql_ignore_locked(conn, "ALTER TABLE coverage ADD COLUMN market VARCHAR(20) DEFAULT 'futures_um'")
                self._exec_sql_ignore_locked(conn, "CREATE INDEX IF NOT EXISTS ix_coverage_market ON coverage (market)")
            if "cadence" not in cols:
                self._exec_sql_ignore_locked(conn, "ALTER TABLE coverage ADD COLUMN cadence VARCHAR(20) DEFAULT 'daily'")
                self._exec_sql_ignore_locked(conn, "CREATE INDEX IF NOT EXISTS ix_coverage_cadence ON coverage (cadence)")
            # backfill (SQLite 既有 rows 可能是 NULL)
            self._exec_sql_ignore_locked(conn, "UPDATE coverage SET market='futures_um' WHERE market IS NULL OR market=''")
            self._exec_sql_ignore_locked(conn, "UPDATE coverage SET cadence='daily' WHERE cadence IS NULL OR cadence=''")
    
    def get_session(self) -> Session:
        """取得資料庫 session"""
        return self.Session()
    
    def register_dataset(self, dataset_type: str, market: str = "um", 
                        cadence: str = "daily", requires_interval: bool = False,
                        is_event_stream: bool = False, description: str = None):
        """註冊資料集類型"""
        with self.get_session() as session:
            dataset = session.query(Dataset).filter_by(dataset_type=dataset_type).first()
            if not dataset:
                dataset = Dataset(
                    dataset_type=dataset_type,
                    market=market,
                    cadence=cadence,
                    requires_interval=requires_interval,
                    is_event_stream=is_event_stream,
                    description=description
                )
                session.add(dataset)
                session.commit()
            return dataset
    
    def register_symbol(self, symbol: str, market: str = "um", 
                       listed_at: Optional[datetime] = None, status: str = "TRADING"):
        """註冊交易對"""
        with self.get_session() as session:
            sym = session.query(Symbol).filter_by(symbol=symbol).first()
            if not sym:
                sym = Symbol(
                    symbol=symbol,
                    market=market,
                    listed_at=listed_at,
                    status=status
                )
                session.add(sym)
                session.commit()
            return sym
    
    def upsert_file(self, dataset_type: str, symbol: str, date: str, cadence: str,
                   remote_url: str, exists: bool = False, interval: str = None,
                   remote_size: Optional[int] = None, last_modified: Optional[datetime] = None,
                   etag: Optional[str] = None, notes: Optional[str] = None,
                   market: str = "futures_um"):
        """新增或更新檔案記錄"""
        with self.get_session() as session:
            file = session.query(File).filter_by(
                market=market,
                dataset_type=dataset_type,
                symbol=symbol,
                date=date,
                cadence=cadence,
                interval=interval
            ).first()
            
            if file:
                file.exists = exists
                file.remote_url = remote_url
                if remote_size is not None:
                    file.remote_size = remote_size
                if last_modified is not None:
                    file.last_modified = last_modified
                if etag is not None:
                    file.etag = etag
                if notes is not None:
                    file.notes = notes
                file.updated_at = datetime.utcnow()
            else:
                file = File(
                    market=market,
                    dataset_type=dataset_type,
                    symbol=symbol,
                    date=date,
                    cadence=cadence,
                    interval=interval,
                    remote_url=remote_url,
                    exists=exists,
                    remote_size=remote_size,
                    last_modified=last_modified,
                    etag=etag,
                    notes=notes
                )
                session.add(file)
            
            session.commit()
            return file


def init_database(db_path: str = "catalog.db"):
    """初始化資料庫"""
    db = CatalogDB(db_path)
    db.init_database()
    return db

