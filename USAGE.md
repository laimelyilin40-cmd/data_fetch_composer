# 使用說明

## 快速開始

### 1. 安裝依賴

```powershell
python -m pip install -r requirements.txt
```

> **注意**：在 Windows PowerShell 中，如果 `pip` 命令無法識別，請使用 `python -m pip` 替代。

### 2. 建立資料目錄（Catalog）

首先，你需要盤點可用的資料。這一步**不需要下載資料**，只是檢查遠端有哪些檔案。

**推薦方式：使用簡化腳本**

```powershell
python setup_catalog.py
```

腳本會引導你輸入參數，或直接按 Enter 使用預設設定。

**進階方式：使用 CLI 命令**

```powershell
python -m cli.main catalog-build --symbols BTCUSDT ETHUSDT BNBUSDT --start 2023-01-01 --end 2023-12-31 --dataset-types klines metrics --intervals 1h 1d
```

這會建立 `catalog.db`，記錄所有可用的檔案資訊。

### 3. 查看 Coverage

查看資料覆蓋率：

**推薦方式：使用簡化腳本**

```powershell
python 查看coverage.py
```

**進階方式：使用 CLI 命令**

```powershell
# 查看 Coverage Matrix
python -m cli.main coverage --symbols BTCUSDT ETHUSDT --dataset-types klines metrics
```

### 4. 探勘 Schema

在組裝資料前，需要先了解每種資料的結構：

```bash
python -m cli.main schema-inspect \
  --dataset-type klines \
  --sample-urls "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2023-01-01.zip"
```

### 5. 執行 Recipe

建立一個 recipe 檔案（見 `recipes/example_klines_metrics.yaml`），然後執行：

```bash
python -m cli.main recipe-execute \
  --recipe recipes/example_klines_metrics.yaml
```

## 使用 UI

啟動 Streamlit UI：

**推薦方式：使用簡化腳本**

```powershell
python run_ui.py
```

**或直接使用 Streamlit**

```powershell
streamlit run ui/app.py
```

在瀏覽器中打開 `http://localhost:8501`，你可以：

- **Coverage Matrix**：視覺化查看資料覆蓋率
- **Schema Dictionary**：查看每種資料的欄位定義
- **Recipe Composer**：未來將提供視覺化 recipe 建立介面

## Recipe 格式說明

Recipe 是 YAML 或 JSON 格式的配置檔案，定義如何組裝資料：

```yaml
name: my_dataset
symbols:
  - BTCUSDT
  - ETHUSDT

time_range:
  start: "2023-01-01"
  end: "2023-12-31"

inputs:
  - dataset_type: klines
    interval: "1h"
    columns:
      - open_time
      - open
      - high
      - low
      - close
      - volume
    resample: false
  
  - dataset_type: metrics
    columns:
      - open_time
      - open_interest

join_policy:
  key: open_time
  missing: drop

output_format: long  # 或 wide

output_store:
  format: parquet
  partition_by:
    - symbol
    - date
```

## 複雜資料處理

系統內建了對以下複雜資料的專屬處理：

### bookTicker
- 自動排序（event_time, update_id）
- 去重
- 計算 spread 和 mid price

### aggTrades
- 處理 zip 內巢狀目錄
- 處理同名 CSV 多份的情況
- 產生 manifest 記錄異常

### trades
- 保留原始事件流
- 可選的 resample 功能（轉換為 K 線）

### bookDepth
- 標記為衍生指標（非原始 L2）
- 保留完整資料供後續分析

## 資料庫說明

系統使用兩個 SQLite 資料庫：

- **catalog.db**：儲存檔案索引、coverage 資訊
- **schema.db**：儲存資料結構定義、樣本記錄

這些資料庫會自動建立，無需手動初始化。

## 注意事項

1. **首次使用**：建立 catalog 可能需要一些時間（取決於檢查的檔案數量）
2. **下載資料**：只有在執行 recipe 時才會下載資料
3. **儲存空間**：確保有足夠的空間儲存下載的資料
4. **網路連線**：需要穩定的網路連線來訪問 data.binance.vision

