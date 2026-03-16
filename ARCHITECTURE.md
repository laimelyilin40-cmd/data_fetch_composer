# 系統架構說明

## 核心設計理念

本系統採用**「先盤點、後下載、再組裝」**的三階段設計：

1. **Data Catalog（盤點）**：無需下載即可知道有哪些資料、缺哪些資料
2. **Schema Registry（表頭註冊）**：自動探勘並記錄每種資料的結構
3. **Dataset Composer（組裝）**：用 Recipe 自由組合多種資料源

## 模組架構

### 1. Catalog 模組 (`src/catalog/`)

**database.py**
- 定義 SQLite schema：`Dataset`, `Symbol`, `File`, `Coverage`
- 提供 `CatalogDB` 類別管理資料庫操作

**builder.py**
- `CatalogBuilder`：從遠端盤點資料
- 支援 HEAD 請求檢查檔案存在性
- 自動更新 coverage 摘要

**coverage.py**
- `CoverageAnalyzer`：提供 coverage 分析
- 支援 Matrix 視圖和缺失日期查詢

### 2. Schema 模組 (`src/schema/`)

**registry.py**
- `SchemaRegistry`：儲存資料結構定義
- 記錄欄位、型別、時間鍵、join 建議

**inspector.py**
- `SchemaInspector`：自動探勘資料結構
- 下載樣本檔案並推斷 schema
- 與已知 schema 做一致性校驗

### 3. Downloader 模組 (`src/downloader/`)

**client.py**
- `DownloadClient`：基礎下載功能
- 支援斷點續傳、ETag 驗證

**manager.py**
- `DownloadManager`：管理批量下載
- 與 catalog 整合，追蹤下載狀態

### 4. Processors 模組 (`src/processors/`)

專屬處理器，處理複雜資料：

- **bookticker.py**：排序、去重、計算衍生欄位
- **aggtrades.py**：處理 zip 異常、產生 manifest
- **trades.py**：事件流處理、可選 resample
- **bookdepth.py**：標記為衍生指標

### 5. Composer 模組 (`src/composer/`)

**recipe.py**
- `Recipe`：Recipe 定義（Pydantic model）
- 支援 YAML/JSON 格式

**validator.py**
- `RecipeValidator`：驗證 recipe
- 檢查 coverage、schema、join key

**merger.py**
- `DatasetMerger`：執行 recipe
- 下載 → 處理 → 合併 → 格式化 → 儲存

### 6. Utils 模組 (`src/utils/`)

工具函數：
- `time_utils.py`：日期處理
- `file_utils.py`：檔案操作

## 資料流程

### Catalog 建立流程

```
1. CatalogBuilder.build_catalog()
   ↓
2. 對每個 (symbol, dataset_type, date, interval) 組合
   ↓
3. 發送 HEAD 請求檢查檔案
   ↓
4. 更新 File 表
   ↓
5. 計算 Coverage 摘要
```

### Recipe 執行流程

```
1. 載入 Recipe
   ↓
2. RecipeValidator.validate()
   - 檢查 coverage
   - 檢查 schema
   - 檢查 join key
   ↓
3. DatasetMerger.execute_recipe()
   - 下載資料
   - 使用專屬處理器處理
   - 選取指定欄位
   - 合併 DataFrame
   - 格式化輸出（long/wide）
   - 儲存為 parquet
   - 產生 manifest
```

## 資料庫 Schema

### catalog.db

**datasets 表**
- 資料集類型定義
- 是否需要 interval、是否為事件流

**symbols 表**
- 交易對資訊

**files 表**（核心表）
- 檔案索引：dataset_type, symbol, interval, date, cadence
- 遠端資訊：url, exists, size, last_modified, etag
- 本地資訊：local_path, downloaded_at
- 品質註記：notes

**coverage 表**
- 摘要資訊：start_date, end_date, num_files, missing_dates

### schema.db

**schemas 表**
- 資料結構定義：columns, dtypes, primary_time_key, join_key
- 驗證狀態：expected_schema, validation_status

**samples 表**
- 樣本檔案記錄：sample_file_url, first_n_rows

## 已知限制與未來改進

### 當前限制

1. **遠端目錄 listing**：目前使用 HEAD 請求，未來可優化為批次檢查
2. **Resample 功能**：trades resample 功能較簡化
3. **Wide format**：需要更完整的 pivot 實作
4. **錯誤處理**：部分錯誤處理較簡化

### 未來改進方向

1. **並行下載**：支援多執行緒/非同步下載
2. **增量更新**：只檢查新增/變更的檔案
3. **快取機制**：快取 schema 和 coverage 查詢
4. **更多視覺化**：在 UI 中提供更多圖表
5. **Recipe 模板**：提供常用 recipe 模板
6. **資料驗證**：更嚴格的資料品質檢查

## 技術選型

- **資料處理**：Polars（高效能 DataFrame）
- **資料庫**：SQLite（輕量、易用）
- **UI**：Streamlit（快速開發）
- **配置**：Pydantic（型別安全）
- **序列化**：YAML/JSON

## 擴展性

系統設計為模組化，易於擴展：

1. **新增資料集類型**：在 `CatalogBuilder.DATASET_TYPES` 添加
2. **新增處理器**：在 `processors/` 添加，並在 `DatasetMerger.PROCESSORS` 註冊
3. **新增輸出格式**：在 `DatasetMerger._format_output()` 擴展
4. **自訂驗證規則**：在 `RecipeValidator` 添加

