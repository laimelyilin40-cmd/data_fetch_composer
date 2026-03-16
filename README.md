# Binance Vision 二維時序資料處理平台

這個專案的核心在於把分散的原始檔案整理成可直接研究的二維時序表。

Binance Vision 的資料天然分散在不同市場、資料集、interval 與 cadence。實際研究流程需要的是一張已經完成時間對齊、可以直接加欄位、做統計、做特徵工程的寬表。這個專案圍繞這個目標建立了完整流程：遠端探測、快取、對齊、拼貼、公式運算與報告輸出。

資料處理的重點放在兩個方向：

- 縱向運算：沿時間軸處理單一欄位，例如位移、差分、報酬率、rolling 與平滑
- 橫向運算：在同一個 timestamp 下跨多欄位或多標的計算，例如同列平均、分位數、排名與 cross-sectional 標準化

因此，這個專案真正要解決的是如何把原始市場資料轉成可反覆運算的二維研究資料。

## 如何啟動

### 1. 安裝依賴

```bash
python -m pip install -r requirements.txt
```

### 2. 啟動 UI

啟動指令：

```bash
python run_ui.py
```

預設位址：

- `http://127.0.0.1:8511`

也可直接使用 Streamlit：

```bash
python -m streamlit run ui/app.py
```

### 3. 首次使用順序

若目標是直接產出資料集，可直接進入 `Recipe Composer -> Dataset Builder`，不需要先建立 coverage。

可先用一組穩定來源驗證流程：

- `market`: `futures_um`
- `symbol`: `BTCUSDT`
- `dataset`: `klines`
- `interval`: `1h` 或 `15m`
- `cadence`: `daily`

完成一次資料拼貼後，再進入 `Table Paster` 做二維欄位運算。

### 4. 何時需要先建 metadata

若需要完整的下拉選單、coverage 預覽與 schema 資訊，可先執行：

```bash
python build_menu.py
```

這條流程主要服務：

- `Data Menu`
- `Coverage Matrix`
- `Schema Dictionary`

這是 metadata 建置流程，不屬於資料拼貼主流程的硬前置。

## 專案重點

本專案主要展示四件事：

- 把分散的遠端市場檔案整理成時間對齊的研究資料表
- 將 metadata 掃描與實際資料拼貼拆成兩條流程
- 以 parquet 作為中介快取，降低重複下載與反覆實驗成本
- 在 Polars 上建立支援縱向與橫向運算的安全公式系統

## 這個專案在解什麼問題

原始市場資料常見的工程問題包括：

- 檔案分散在不同目錄規則之下
- 同一份研究資料需要混合多個來源
- metadata 掃描成本與實際下載成本不同
- 網路品質不穩時，完整建立 coverage 很耗時
- 缺資料、404、網路錯誤與解析錯誤需要分開追蹤

專案把這些問題拆成兩層處理：

- 預覽層：先看大致有哪些資料、欄位與 coverage
- 執行層：依條件直接拼資料，並把執行結果寫入 report

## 核心能力

### 1. Dataset Builder

`Dataset Builder` 會根據：

- `market`
- `symbol`
- `dataset`
- `cadence`
- `interval`
- date range

動態推導 Binance Vision 的候選 URL，然後完成以下工作：

1. 遠端探測
2. 檔案下載
3. zip 轉 parquet 快取
4. 多來源時間對齊
5. 合併輸出 parquet
6. 寫出 `report.json`

這條流程目前已不再依賴 `catalog.db.files` 才能運作，資料拼貼可以直接依執行條件完成。

### 2. Table Paster

`Table Paster` 建立在已對齊好的 parquet 寬表上，負責把欄位運算固定成可重現的公式流程。

這一層提供：

- 公式加欄
- 批次套用模板
- 新欄位預覽
- 輸出完整 parquet 或僅輸出新欄位
- 公式與 recipe 一併保存

### 3. metadata 瀏覽

UI 內另外保留三個偏向可觀察性的模組：

- `Data Menu`
- `Coverage Matrix`
- `Schema Dictionary`

這些頁面用來快速了解：

- 某個 dataset 是否有對應 interval
- coverage 大致範圍
- 欄位名稱、型別與時間鍵

## 二維運算模型

這個專案的運算核心是一張已對齊的寬表。這張表建立之後，可以在同一個資料平面上同時進行時間序列運算與 cross-sectional 運算。

### 縱向運算

縱向運算沿著時間軸處理單一欄位，對應的常見函數包括：

- `LAG`
- `DIFF`
- `PCT_CHANGE`
- `LOGRET`
- `ROLL_MEAN`
- `ROLL_STD`
- `ROLL_SUM`
- `ROLL_ZSCORE`
- `EMA`
- `FILL_FFILL`
- `FILL_BFILL`

適合處理：

- 單欄位報酬率
- rolling 波動
- 平滑訊號
- 缺值補齊

### 橫向運算

橫向運算發生在同一個 timestamp 的多欄位之間，對應的常見函數包括：

- `ROW_SUM`
- `ROW_MEAN`
- `ROW_MIN`
- `ROW_MAX`
- `ROW_STD`
- `ROW_VAR`
- `ROW_MEDIAN`
- `ROW_QUANTILE`
- `ROW_TOPK_MEAN`
- `ROW_BOTTOMK_MEAN`
- `ROW_COUNT_VALID`

適合處理：

- 多標的同列聚合
- 同列離散程度估計
- 同一時間點的欄位組合特徵

### Cross-sectional 運算

針對同一個 timestamp 下的跨標的比較，公式系統另外提供：

- `XS_DEMEAN`
- `XS_ZSCORE`
- `XS_RANK`
- `XS_PCTRANK`
- `SOFTMAX_WEIGHT`

這一層讓同列正規化、排序與權重分配可以直接在寬表中完成。

## 公式系統

公式系統的目標是把研究常用的欄位運算固定成一個安全、可追蹤、可重複使用的介面。

主要特性包括：

- 公式先經過 AST 檢查
- 禁止任意 Python 執行
- 編譯結果是 Polars Expr
- 支援欄位依賴解析與自動排序
- 支援 `COL("name")` 與 `COLS("regex")`

這使得公式可以像工作表一樣逐層堆疊，但仍保留向量化執行的效率與可控性。

詳細函數表詳見 `FORMULA_TABLE.md`。

## 輸出與可追溯性

每次資料拼貼都會輸出以下產物：

- `data/outputs/datasets/<name>/<name>.parquet`
- `data/outputs/datasets/<name>/<name>.report.json`
- `data/outputs/datasets/<name>/<name>.recipe.json`

`Table Paster` 另外會在輸出資料夾保留：

- `formulas.tsv`
- `recipe.json`
- `report.json`

報告內容會區分：

- 成功下載數
- `404`
- 網路錯誤
- 解析錯誤
- 最終欄位
- row count
- 每個 selection 的摘要

這一層讓資料缺漏、上游不可用與解析異常不會混在同一個失敗訊號裡。

## 設計取捨

### coverage 與實際拼貼分離

專案原本偏向先完成 coverage，再開始資料組裝。這在網路穩定時可行，但實務上會被大量 HEAD 探測拖慢，也會讓資料拼貼過度依賴 metadata 是否先準備完成。

目前的做法是把 coverage 定位為預覽資訊，而把實際資料拼貼改成執行期直接推導 URL、直接下載、直接寫 report。這樣可以先完成真正需要的資料表，再回頭檢查缺漏原因。

### parquet 作為中介層

原始 zip 第一次下載後會轉成 parquet 快取。這個設計讓：

- 同一批來源反覆實驗時不需要重抓
- 對齊與合併能在更穩定的格式上完成
- 後續公式運算可直接建立在 parquet 上

### schema 保留為輔助資訊

`schema.db` 對欄位選取與時間鍵推斷很有幫助，但實際拼貼流程已可在 schema 不完整時採用保守推斷，因此不再是主流程的硬前置。

## 系統結構

### `src/cache/`

- `raw_cache.py`
  - 規劃遠端 URL
  - 探測檔案存在性
  - 下載原始資料
  - 建立 parquet 快取與 manifest

### `src/composer/`

- `interactive_builder.py`
  - 讀取快取
  - 多來源對齊
  - 主時間軸合併
  - 補值
  - parquet 與 report 輸出

### `src/features/`

- `formula_engine.py`
  - 公式 AST 安全檢查
  - 欄位依賴排序
  - Polars Expr 編譯
  - 縱向、橫向與 cross-sectional 函數實作

### `ui/`

- `app.py`
  - 主介面
- `table_paster.py`
  - 公式加欄、預覽與輸出介面

### `src/catalog/` / `src/schema/`

這兩個模組主要支援 metadata、coverage 與 schema 瀏覽。

## 作品集閱讀重點

若從工程實作角度閱讀，最值得優先查看的模組是：

- `src/cache/raw_cache.py`
- `src/composer/interactive_builder.py`
- `src/features/formula_engine.py`
- `ui/table_paster.py`

這幾個模組最能代表本專案的核心能力：

- metadata 與 assembly 解耦
- parquet 中介快取
- 二維時序資料處理
- 安全、向量化的公式系統

## 相關文件

- `FORMULA_TABLE.md`：公式系統完整參考
- `QUICKSTART.md`：導向本 README
- `USAGE.md`：導向本 README
- `ARCHITECTURE.md`：導向本 README
