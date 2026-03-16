# Binance Vision 市場資料統整系統

一個完整的 Binance Vision (`data.binance.vision`) 資料盤點、下載、合併與可視化系統。

## 核心功能

### 1. Data Catalog（資料盤點）
- 無需下載即可查看可用資料
- Coverage Matrix 視覺化
- 缺失日期追蹤

### 2. Schema Registry（表頭註冊）
- 自動探勘資料結構
- 欄位字典與型別推斷
- 不同 klines 類型的語義區分

### 3. Dataset Composer（資料組裝）
- Recipe 驅動的資料組合
- 支援多資料源合併
- 輸出 long/wide format

### 4. 複雜資料處理
- bookTicker：排序與去重
- aggTrades：zip 異常處理
- trades：事件流處理
- bookDepth：衍生指標識別

## 專案結構

```
量化交易系統/
├── src/
│   ├── __init__.py
│   ├── catalog/
│   │   ├── __init__.py
│   │   ├── database.py      # SQLite schema 與 ORM
│   │   ├── builder.py        # 資料盤點建置器
│   │   └── coverage.py       # Coverage 分析
│   ├── schema/
│   │   ├── __init__.py
│   │   ├── inspector.py      # Schema 探勘
│   │   └── registry.py       # Schema 註冊表
│   ├── downloader/
│   │   ├── __init__.py
│   │   ├── client.py         # 下載客戶端
│   │   └── manager.py        # 下載管理器
│   ├── processors/
│   │   ├── __init__.py
│   │   ├── bookticker.py
│   │   ├── aggtrades.py
│   │   ├── trades.py
│   │   └── bookdepth.py
│   ├── composer/
│   │   ├── __init__.py
│   │   ├── recipe.py         # Recipe 解析
│   │   ├── validator.py      # 驗證器
│   │   └── merger.py         # 資料合併
│   └── utils/
│       ├── __init__.py
│       ├── time_utils.py
│       └── file_utils.py
├── cli/
│   ├── __init__.py
│   └── main.py               # CLI 入口
├── ui/
│   ├── __init__.py
│   └── app.py                # Streamlit UI
├── recipes/
│   └── example_klines_metrics.yaml
├── tests/
│   └── test_catalog.py
├── requirements.txt
└── README.md
```

## 快速開始

### 安裝依賴

```powershell
python -m pip install -r requirements.txt
```

> **注意**：在 Windows PowerShell 中，如果 `pip` 命令無法識別，請使用 `python -m pip` 替代。

### 建立你要的「大菜單」（前十大、Spot + Futures UM）

```powershell
python build_menu.py
```

### 建立資料目錄

**推薦方式（最簡單）：**

```powershell
python setup_catalog.py
```

**或使用 CLI：**

```powershell
python -m cli.main catalog-build --symbols BTCUSDT ETHUSDT --start 2023-01-01 --end 2023-12-31
```

### 啟動 UI

**推薦方式（最簡單）：**

```powershell
python run_ui.py
```

**或直接使用 Streamlit：**

```powershell
streamlit run ui/app.py
```

## 使用範例

### Recipe 範例

見 `recipes/example_klines_metrics.yaml`

