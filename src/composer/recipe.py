"""
Recipe 定義與解析
"""

from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field
import yaml
import json
from pathlib import Path


class InputSource(BaseModel):
    """輸入資料源定義"""
    dataset_type: str
    interval: Optional[str] = None
    columns: List[str]  # 要選取的欄位
    resample: bool = False  # 是否需要 resample
    resample_freq: Optional[str] = None  # 例如 "1m", "5m"


class Recipe(BaseModel):
    """Recipe 定義"""
    name: str
    description: Optional[str] = None
    
    symbols: List[str]
    time_range: Dict[str, str]  # {"start": "2023-01-01", "end": "2023-12-31"}
    
    inputs: List[InputSource]
    
    join_policy: Dict[str, Any] = Field(default_factory=lambda: {
        "key": "open_time",  # 或 "ts", "timestamp"
        "missing": "drop"  # drop, ffill, keep_nan
    })
    
    output_format: str = "long"  # long 或 wide
    output_store: Dict[str, Any] = Field(default_factory=lambda: {
        "format": "parquet",
        "partition_by": ["symbol", "date"]  # 分區規則
    })
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> "Recipe":
        """從 YAML 檔案載入"""
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_path: str) -> "Recipe":
        """從 JSON 檔案載入"""
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls(**data)
    
    def to_dict(self) -> Dict[str, Any]:
        """轉換為字典"""
        return self.model_dump()
    
    def to_yaml(self, yaml_path: str):
        """儲存為 YAML"""
        with open(yaml_path, "w", encoding="utf-8") as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, allow_unicode=True)
    
    def to_json(self, json_path: str):
        """儲存為 JSON"""
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)


def load_recipe(recipe_path: str) -> Recipe:
    """載入 recipe（自動判斷格式）"""
    path = Path(recipe_path)
    if path.suffix.lower() == ".yaml" or path.suffix.lower() == ".yml":
        return Recipe.from_yaml(recipe_path)
    elif path.suffix.lower() == ".json":
        return Recipe.from_json(recipe_path)
    else:
        raise ValueError(f"Unsupported recipe format: {path.suffix}")

