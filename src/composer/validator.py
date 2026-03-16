"""
Recipe 驗證器
"""

from typing import List, Dict, Optional, Tuple
from .recipe import Recipe
from ..catalog.database import CatalogDB
from ..schema.registry import SchemaRegistry
from ..catalog.coverage import CoverageAnalyzer


class RecipeValidator:
    """Recipe 驗證器"""
    
    def __init__(self, catalog_db: CatalogDB, schema_registry: SchemaRegistry):
        self.catalog_db = catalog_db
        self.schema_registry = schema_registry
        self.coverage_analyzer = CoverageAnalyzer(catalog_db)
    
    def validate(self, recipe: Recipe) -> Tuple[bool, List[str]]:
        """
        驗證 recipe
        
        Returns:
            (is_valid, errors) - 是否有效與錯誤列表
        """
        errors = []
        
        # 驗證 1: Coverage 檢查
        coverage_errors = self._validate_coverage(recipe)
        errors.extend(coverage_errors)
        
        # 驗證 2: Schema 檢查
        schema_errors = self._validate_schemas(recipe)
        errors.extend(schema_errors)
        
        # 驗證 3: Join key 檢查
        join_errors = self._validate_join_keys(recipe)
        errors.extend(join_errors)
        
        return len(errors) == 0, errors
    
    def _validate_coverage(self, recipe: Recipe) -> List[str]:
        """驗證資料覆蓋率"""
        errors = []
        start_date = recipe.time_range["start"]
        end_date = recipe.time_range["end"]
        
        for symbol in recipe.symbols:
            for input_source in recipe.inputs:
                missing = self.coverage_analyzer.get_missing_dates(
                    symbol=symbol,
                    dataset_type=input_source.dataset_type,
                    start_date=start_date,
                    end_date=end_date,
                    interval=input_source.interval
                )
                
                if missing:
                    errors.append(
                        f"{symbol} {input_source.dataset_type} "
                        f"missing dates: {missing[:10]}{'...' if len(missing) > 10 else ''}"
                    )
        
        return errors
    
    def _validate_schemas(self, recipe: Recipe) -> List[str]:
        """驗證 schema（欄位是否存在）"""
        errors = []
        
        for input_source in recipe.inputs:
            schema = self.schema_registry.get_schema(input_source.dataset_type)
            
            if not schema:
                errors.append(
                    f"Schema not found for {input_source.dataset_type}. "
                    "Please run schema inspection first."
                )
                continue
            
            available_columns = {col["name"] for col in schema["columns"]}
            requested_columns = set(input_source.columns)
            
            missing_columns = requested_columns - available_columns
            if missing_columns:
                errors.append(
                    f"{input_source.dataset_type} missing columns: {missing_columns}. "
                    f"Available: {sorted(available_columns)}"
                )
        
        return errors
    
    def _validate_join_keys(self, recipe: Recipe) -> List[str]:
        """驗證 join key 是否在所有資料源中存在"""
        errors = []
        join_key = recipe.join_policy.get("key", "open_time")
        
        for input_source in recipe.inputs:
            schema = self.schema_registry.get_schema(input_source.dataset_type)
            if schema:
                available_columns = {col["name"] for col in schema["columns"]}
                if join_key not in available_columns:
                    # 檢查是否有建議的 join_key
                    suggested_key = schema.get("join_key") or schema.get("primary_time_key")
                    if suggested_key:
                        errors.append(
                            f"{input_source.dataset_type} does not have join key '{join_key}'. "
                            f"Suggested: '{suggested_key}'"
                        )
                    else:
                        errors.append(
                            f"{input_source.dataset_type} does not have join key '{join_key}'"
                        )
        
        return errors

