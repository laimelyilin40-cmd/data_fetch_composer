````markdown
# Binance Vision 研究資料處理工具

本工具為個人量化研究流程開發，目的是將 Binance Vision 上分散的原始檔案整理為可直接分析的二維時序寬表。

研究流程真正需要的資料形態，通常不是單一原始檔，而是一張已完成時間對齊、可直接加欄位、做統計分析與特徵工程的寬表。本工具即圍繞這個需求設計，涵蓋遠端探測、下載快取、資料對齊、表格拼貼、公式運算與報告輸出。

## 這個工具在做什麼

本工具的核心工作是把分散的市場資料整理成研究可直接使用的表格。重點不在資料展示，而在資料可用性與研究流程可重現性。

主要處理內容包括：

- 根據條件推導 Binance Vision 遠端檔案位置
- 下載並快取原始資料
- 將原始 zip 轉成 parquet
- 對多來源資料做時間對齊
- 合併成研究可用的寬表
- 在寬表上進行欄位公式運算
- 輸出資料、recipe 與執行報告

## 為什麼需要這個工具

Binance Vision 的資料天然分散在不同 market、dataset、interval 與 cadence 之下。研究流程若直接面對原始資料，常見問題包括：

- 檔案分散在不同目錄規則下
- 同一份研究資料需要混合多個來源
- metadata 掃描與實際下載屬於不同流程
- 網路品質不穩時，完整 coverage 建置成本偏高
- 缺資料、404、網路錯誤與解析錯誤需要分開追蹤
- 原始壓縮檔不適合作為反覆實驗的直接輸入格式

本工具將上述問題拆成兩層：

- 預覽層：查看 coverage、schema 與欄位資訊
- 執行層：依條件直接拼資料並輸出結果

## 核心能力

### 1. Dataset Builder

`Dataset Builder` 是主資料拼貼流程，根據以下條件動態產出資料集：

- `market`
- `symbol`
- `dataset`
- `cadence`
- `interval`
- date range

執行內容包括：

1. 推導候選 URL
2. 探測遠端檔案
3. 下載原始 zip
4. 轉成 parquet 快取
5. 多來源時間對齊
6. 合併輸出最終寬表
7. 寫出 `report.json`

這條流程可直接執行，不需要先建立完整 coverage。

### 2. Table Paster

`Table Paster` 建立在已完成對齊的 parquet 寬表上，負責將欄位運算固定成可重現的公式流程。

主要功能包括：

- 公式加欄
- 批次套用模板
- 新欄位預覽
- 輸出完整 parquet
- 僅輸出新增欄位
- 保存 formulas、recipe 與 report

這一層的定位是研究資料表上的特徵工程工具。

### 3. Metadata 瀏覽模組

UI 內另外保留三個偏向可觀察性的模組：

- `Data Menu`
- `Coverage Matrix`
- `Schema Dictionary`

這些頁面用來快速了解：

- 某個 dataset 是否存在對應 interval
- coverage 大致範圍
- 欄位名稱、型別與時間鍵資訊

## 資料運算模型

本工具的運算核心是一張已完成時間對齊的寬表。這張表建立後，可在同一個資料平面上同時進行時間序列運算與 cross-sectional 運算。

### 縱向運算

縱向運算沿時間軸處理單一欄位，常見函數包括：

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
- rolling 波動估計
- 平滑訊號
- 缺值補齊

### 橫向運算

橫向運算發生在同一個 timestamp 下的多欄位之間，常見函數包括：

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

適合處理：

- 同列正規化
- 同列排序
- 權重分配
- cross-sectional signal construction

## 公式系統

公式系統的目標是將研究常用的欄位運算固定成安全、可追蹤、可重複使用的介面。

主要特性包括：

- 公式先經過 AST 檢查
- 禁止任意 Python 執行
- 編譯結果為 Polars Expr
- 支援欄位依賴解析與自動排序
- 支援 `COL("name")`
- 支援 `COLS("regex")`

這使公式可以像工作表一樣逐層堆疊，同時保留向量化執行效率與可控性。

完整函數表見 `FORMULA_TABLE.md`。

## 系統流程

整體流程可概括為：

`Binance Vision ZIP -> URL probing -> download -> parquet cache -> align/merge -> wide table -> formula engine -> outputs/report`

更細的流程如下：

1. 依照輸入條件推導遠端候選 URL
2. 探測檔案存在性
3. 下載原始資料
4. 建立 parquet 快取
5. 對多來源資料做時間對齊
6. 合併成最終寬表
7. 輸出 parquet、recipe 與 report
8. 在寬表上進一步做公式加欄

## 如何啟動

### 1. 安裝依賴

```bash
python -m pip install -r requirements.txt
````

### 2. 啟動 UI

```bash
python run_ui.py
```

預設位址：

* `http://127.0.0.1:8511`

也可直接使用 Streamlit：

```bash
python -m streamlit run ui/app.py
```

## 建議的首次使用流程

若目標是直接產出資料集，可直接進入 `Recipe Composer -> Dataset Builder`。

建議先使用一組穩定來源驗證流程：

* `market`: `futures_um`
* `symbol`: `BTCUSDT`
* `dataset`: `klines`
* `interval`: `1h` 或 `15m`
* `cadence`: `daily`

完成一次資料拼貼後，再進入 `Table Paster` 進行欄位運算。

## 何時需要先建 metadata

若需要完整的下拉選單、coverage 預覽與 schema 資訊，可先執行：

```bash
python build_menu.py
```

此流程主要服務：

* `Data Menu`
* `Coverage Matrix`
* `Schema Dictionary`

這是 metadata 建置流程，不屬於資料拼貼主流程的必要前置。

## 輸出內容

每次資料拼貼會輸出以下產物：

* `data/outputs/datasets/<name>/<name>.parquet`
* `data/outputs/datasets/<name>/<name>.report.json`
* `data/outputs/datasets/<name>/<name>.recipe.json`

`Table Paster` 另外會保留：

* `formulas.tsv`
* `recipe.json`
* `report.json`

報告內容包含：

* 成功下載數
* `404`
* 網路錯誤
* 解析錯誤
* 最終欄位
* row count
* 每個 selection 的摘要

這一層可保留資料缺漏與執行異常的完整紀錄，方便追蹤與除錯。

## 設計原則

### Coverage 與資料拼貼分離

coverage 被定位為預覽資訊。資料拼貼被定位為獨立執行流程。這樣可先完成真正需要的研究資料表，再檢查 coverage 與缺漏原因。

### Parquet 作為中介快取層

原始 zip 第一次下載後會轉成 parquet 快取。這使同一批來源在反覆實驗時不需要重抓，對齊與合併也能建立在更穩定的格式上。

### Schema 作為輔助資訊

`schema.db` 對欄位選取與時間鍵推斷有幫助。主流程在 schema 不完整時仍可採用保守推斷，因此 schema 被保留為輔助資訊層。

## 專案結構

### `src/cache/`

* `raw_cache.py`

  * 規劃遠端 URL
  * 探測檔案存在性
  * 下載原始資料
  * 建立 parquet 快取與 manifest

### `src/composer/`

* `interactive_builder.py`

  * 讀取快取
  * 多來源對齊
  * 主時間軸合併
  * 補值
  * parquet 與 report 輸出

### `src/features/`

* `formula_engine.py`

  * 公式 AST 安全檢查
  * 欄位依賴排序
  * Polars Expr 編譯
  * 縱向、橫向與 cross-sectional 函數實作

### `ui/`

* `app.py`

  * 主介面
* `table_paster.py`

  * 公式加欄、預覽與輸出介面

### `src/catalog/` / `src/schema/`

這兩個模組主要支援 metadata、coverage 與 schema 瀏覽。

## 相關文件

* `FORMULA_TABLE.md`：公式系統完整參考
* `QUICKSTART.md`：快速啟動說明
* `USAGE.md`：使用方式說明
* `ARCHITECTURE.md`：系統結構說明

