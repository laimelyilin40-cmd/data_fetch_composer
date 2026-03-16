"""
Recipe 模組測試
"""

import pytest
import tempfile
import os
from pathlib import Path

from src.composer.recipe import Recipe, load_recipe


def test_recipe_creation():
    """測試建立 Recipe"""
    recipe = Recipe(
        name="test_recipe",
        symbols=["BTCUSDT"],
        time_range={"start": "2023-01-01", "end": "2023-12-31"},
        inputs=[
            {
                "dataset_type": "klines",
                "interval": "1h",
                "columns": ["open_time", "open", "close"]
            }
        ]
    )
    
    assert recipe.name == "test_recipe"
    assert len(recipe.symbols) == 1
    assert len(recipe.inputs) == 1


def test_recipe_yaml():
    """測試 Recipe YAML 序列化"""
    recipe = Recipe(
        name="test_recipe",
        symbols=["BTCUSDT"],
        time_range={"start": "2023-01-01", "end": "2023-12-31"},
        inputs=[
            {
                "dataset_type": "klines",
                "interval": "1h",
                "columns": ["open_time", "open", "close"]
            }
        ]
    )
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        temp_path = f.name
    
    try:
        recipe.to_yaml(temp_path)
        assert os.path.exists(temp_path)
        
        # 載入
        loaded = load_recipe(temp_path)
        assert loaded.name == recipe.name
        assert loaded.symbols == recipe.symbols
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

