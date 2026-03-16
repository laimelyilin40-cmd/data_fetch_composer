"""
Schema 模組測試
"""

import pytest
import os

from src.schema.registry import SchemaRegistry


def test_init_registry():
    """測試 Schema Registry 初始化"""
    test_db = "test_schema.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    registry = SchemaRegistry(test_db)
    assert os.path.exists(test_db)
    
    # 清理
    os.remove(test_db)


def test_register_schema():
    """測試註冊 schema"""
    test_db = "test_schema.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    registry = SchemaRegistry(test_db)
    
    columns = [
        {"name": "open_time", "dtype": "int64", "position": 0},
        {"name": "open", "dtype": "float64", "position": 1}
    ]
    dtypes = {"open_time": "int64", "open": "float64"}
    
    schema = registry.register_schema(
        dataset_type="klines",
        columns=columns,
        dtypes=dtypes,
        primary_time_key="open_time"
    )
    
    assert schema.dataset_type == "klines"
    assert schema.primary_time_key == "open_time"
    
    # 查詢
    retrieved = registry.get_schema("klines")
    assert retrieved is not None
    assert retrieved["primary_time_key"] == "open_time"
    
    # 清理
    os.remove(test_db)

