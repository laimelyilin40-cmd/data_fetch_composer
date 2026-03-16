"""
Catalog 模組測試
"""

import pytest
import os
from pathlib import Path

from src.catalog.database import init_database, CatalogDB
from src.catalog.builder import CatalogBuilder


def test_init_database():
    """測試資料庫初始化"""
    test_db = "test_catalog.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    db = init_database(test_db)
    assert os.path.exists(test_db)
    
    # 清理
    os.remove(test_db)


def test_register_dataset():
    """測試註冊資料集"""
    test_db = "test_catalog.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    db = init_database(test_db)
    dataset = db.register_dataset("klines", market="um", requires_interval=True)
    assert dataset.dataset_type == "klines"
    assert dataset.requires_interval == True
    
    # 清理
    os.remove(test_db)


def test_register_symbol():
    """測試註冊交易對"""
    test_db = "test_catalog.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    db = init_database(test_db)
    symbol = db.register_symbol("BTCUSDT", market="um")
    assert symbol.symbol == "BTCUSDT"
    
    # 清理
    os.remove(test_db)


def test_upsert_file():
    """測試檔案記錄"""
    test_db = "test_catalog.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    db = init_database(test_db)
    db.register_dataset("klines", requires_interval=True)
    db.register_symbol("BTCUSDT")
    
    file = db.upsert_file(
        dataset_type="klines",
        symbol="BTCUSDT",
        date="2023-01-01",
        cadence="daily",
        interval="1h",
        remote_url="https://example.com/file.zip",
        exists=True,
        remote_size=1024
    )
    
    assert file.dataset_type == "klines"
    assert file.symbol == "BTCUSDT"
    assert file.exists == True
    
    # 清理
    os.remove(test_db)

