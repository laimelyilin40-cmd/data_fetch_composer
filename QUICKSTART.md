# 快速開始指南

## 5 分鐘快速上手

### 步驟 1：安裝依賴

```powershell
python -m pip install -r requirements.txt
```

> **注意**：在 Windows PowerShell 中，如果 `pip` 命令無法識別，請使用 `python -m pip` 替代。

### 🎯 最簡單的方式：使用批次檔選單

如果你在 Windows 上，可以直接雙擊 `快速開始.bat`，會出現選單讓你選擇操作！

### 或者使用 Python 腳本（推薦）

### 步驟 2：建立資料目錄（盤點）

這一步**不需要下載資料**，只是檢查遠端有哪些檔案可用。

**方法 1：使用簡化腳本（推薦）**

```powershell
python setup_catalog.py
```

腳本會引導你輸入參數，或使用預設設定。

**方法 2：使用 CLI 命令**

```powershell
python -m cli.main catalog-build --symbols BTCUSDT --start 2023-01-01 --end 2023-01-31 --dataset-types klines metrics --intervals 1h
```

這會建立 `catalog.db`，記錄所有可用的檔案資訊。

### 步驟 3：查看 Coverage

**方法 1：使用簡化腳本（推薦）**

```powershell
python 查看coverage.py
```

**方法 2：使用 CLI 命令**

```powershell
python -m cli.main coverage --symbols BTCUSDT ETHUSDT
```

### 步驟 4：啟動 UI 視覺化

**方法 1：使用簡化腳本（推薦）**

```powershell
python run_ui.py
```

**方法 2：直接使用 Streamlit**

```powershell
streamlit run ui/app.py
```

在瀏覽器中打開 `http://localhost:8501`，你可以：

- 查看 Coverage Matrix
- 查看 Schema Dictionary
- 未來：使用 Recipe Composer

## 🎛️ 建立你要的「大菜單」（前十大、Spot + Futures UM、全時間範圍 + 表頭）

你想要的下拉式「大菜單」請跑這個一鍵腳本：

```powershell
python build_menu.py
```

完成後開 UI：

```powershell
streamlit run ui/app.py
```

然後到 **Data Menu** 頁面用下拉選單瀏覽：
- Symbol → Dataset → Cadence(daily/monthly) → Interval(若有)
- 會顯示：最早~最晚日期 + 樣本檔 Size/Last-Modified + Schema 欄位表

## 🧰 Dataset Builder（自組裝車）：用 UI 組裝並下載合併後的訓練資料

1) 先確保已建立大菜單與表頭（只要做一次）：

```powershell
python build_menu.py
```

2) 啟動 UI（Windows 建議用 python -m）：

```powershell
python -m streamlit run ui/app.py
```

> 重要：請避免同時開多個 Streamlit（例如 8501/8504），不然你可能會在舊 UI 上操作，導致「明明改了程式卻還是報同樣錯」。

3) 到 UI 左側選單選 **Recipe Composer → Dataset Builder（自組裝車）**：
- 新增清單（可有多個組合）
- 加入來源：Market → Symbol → Dataset → Interval（若有）→ 欄位（全部/勾選）
  - 選「全部」會自動排除 `ignore`
- 選主時間軸（以哪個來源的時間對齊）
- 按「開始產出」：會下載 raw 檔、合併、並輸出
  - Parquet：`data/outputs/datasets/<name>/<name>.parquet`
  - 報告：`data/outputs/datasets/<name>/<name>.report.json`（缺哪些日、補了多少筆、補值策略）

## 🧹 一鍵重置 data/（建議在邏輯混亂或快取污染時使用）

如果你遇到：
- parquet 快取欄位 dtype 不一致（例如 metrics 某些天被推斷成字串）
- 想「全部清乾淨重來」

可以先停止正在跑的 Streamlit（關掉 PowerShell 視窗或按 `Ctrl + C`），再執行：

```powershell
# 先預覽會刪哪些（不會真的刪）
python -m cli.main data-reset

# 真的刪除並重建 data/raw_parquet、data/raw_zips、data/downloads、data/outputs/datasets
python -m cli.main data-reset --yes
```

> 注意：`data/downloads` 裡可能有大量 zip（你目前有數千個），刪除需要一點時間是正常的。

## 完整工作流程

### 1. 盤點資料（Catalog）

```bash
# 盤點多個交易對和資料集
python -m cli.main catalog-build \
  --symbols BTCUSDT ETHUSDT BNBUSDT \
  --start 2023-01-01 \
  --end 2023-12-31 \
  --dataset-types klines metrics bookTicker \
  --intervals 1h 1d \
  --cadence daily
```

### 2. 探勘 Schema

在組裝資料前，先了解資料結構：

```bash
python -m cli.main schema-inspect \
  --dataset-type klines \
  --sample-urls "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2023-01-01.zip"
```

### 3. 建立 Recipe

編輯 `recipes/my_recipe.yaml`：

```yaml
name: my_dataset
symbols:
  - BTCUSDT
time_range:
  start: "2023-01-01"
  end: "2023-01-31"
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
join_policy:
  key: open_time
  missing: drop
output_format: long
```

### 4. 執行 Recipe

```bash
python -m cli.main recipe-execute \
  --recipe recipes/my_recipe.yaml
```

系統會：
1. 驗證 recipe（檢查 coverage、schema）
2. 下載所需資料
3. 處理並合併資料
4. 輸出為 parquet 檔案

## 常見問題

### Q: 建立 catalog 需要多久？

A: 取決於檢查的檔案數量。對於單一交易對、單一資料集、一個月的資料，通常需要 1-2 分鐘。

### Q: 會下載所有資料嗎？

A: 不會。建立 catalog 時只檢查檔案是否存在（HEAD 請求）。只有在執行 recipe 時才會下載資料。

### Q: 如何查看有哪些資料可用？

A: 使用 UI 的 Coverage Matrix，或執行：

```bash
python -m cli.main coverage --symbols BTCUSDT ETHUSDT
```

### Q: 支援哪些資料集類型？

A: 目前支援：
- klines（需要 interval）
- indexPriceKlines（需要 interval）
- markPriceKlines（需要 interval）
- premiumIndexKlines（需要 interval）
- metrics
- trades
- aggTrades
- bookTicker
- bookDepth

### Q: 如何處理複雜資料（bookTicker、aggTrades）？

A: 系統內建了專屬處理器，會自動：
- bookTicker：排序、去重、計算衍生欄位
- aggTrades：處理 zip 異常、去重
- trades：可選 resample
- bookDepth：標記為衍生指標

## 下一步

- 閱讀 [USAGE.md](USAGE.md) 了解詳細使用說明
- 閱讀 [ARCHITECTURE.md](ARCHITECTURE.md) 了解系統架構
- 查看 `recipes/example_klines_metrics.yaml` 了解 Recipe 格式

