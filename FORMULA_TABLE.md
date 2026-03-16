### 表格拼貼器：公式表（MVP 版）

這份表是給 `UI -> Table Paster` 用的「可用語法/函數清單 + 範例」。

> 規則：所有公式都是「整欄向量化」，輸入是欄（序列），輸出也是欄（序列）。

---

### 1) 欄位引用

- **直接用欄位名（推薦）**：如果欄位名是合法變數名（通常你現在的命名都可以）
  - 例：`BTCUSDT_klines_1m_close`
- **用 COL(...)**：欄位名比較奇怪或你想寫得更明確時
  - 例：`COL("BTCUSDT_klines_1m_close")`

---

### 2) 基本運算子（像試算表）

- **算術**：`+  -  *  /  **`
- **比較**：`>  >=  <  <=  ==  !=`
- **邏輯**：`AND  OR  NOT`（也可以用 `and / or / not`）

範例：

- `BTCUSDT_klines_1m_close / LAG(BTCUSDT_klines_1m_close, 1) - 1`
- `IF(BTCUSDT_klines_1m_volume > 0, BTCUSDT_klines_1m_close, NULL)`

---

### 3) 時間序列（沿時間／變著算）

- **LAG(x, n)**：往前 n 根（n>0）
  - 例：`LAG(BTCUSDT_klines_1m_close, 1)`
- **DIFF(x, n)**：差分（delta）
  - 例：`DIFF(BTCUSDT_klines_1m_close, 1)`
- **PCT_CHANGE(x, n)**：百分比變化（simple return）
  - 例：`PCT_CHANGE(BTCUSDT_klines_1m_close, 1)`
- **LOGRET(x, n)**：log return
  - 例：`LOGRET(BTCUSDT_klines_1m_close, 1)`

Rolling：

- **ROLL_MEAN(x, w)** / **ROLL_STD(x, w)** / **ROLL_SUM(x, w)**（min_periods = w）
  - 例：`ROLL_MEAN(BTCUSDT_klines_1m_close, 60)`
  - 例：`ROLL_STD(LOGRET(BTCUSDT_klines_1m_close, 1), 60)`

平滑：

- **EMA(x, span)**
  - 例：`EMA(BTCUSDT_klines_1m_close, 60)`

---

### 4) 缺值處理（對齊不同時間尺度最常用）

> 你的 funding / OI / metrics 這類低頻欄位，在對齊到 1m kline 時會有很多空值；常見作法是「維持最新值」。

- **FILL_FFILL(x)**：forward fill（保持最新值）
  - 例：`FILL_FFILL(BTCUSDT_metrics_sum_open_interest)`

配合運算：

- 例：`LOGRET(FILL_FFILL(BTCUSDT_metrics_sum_open_interest), 1)`

---

### 4.2) 其他常用填補 / 資料品質

- **FILL_BFILL(x)**：backfill
- **FILL_ZERO(x)**：null -> 0
- **ISNA(x)**：是否為 null（回傳布林序列）
- **ISFINITE(x)**：是否為有限值（浮點）

---

### 4.5) 全欄位公式（批次套用）

在 `Table Paster` 的「全欄位公式」區塊，你可以**先選一批欄位（通常是你已經寫好的特徵公式）**，再用同一個模板批次產生更多欄位。

- **模板一定要包含 `{x}`**（代表當前被套用的那一欄）
- 你可以選：
  - **套在既有公式外層（推薦）**：把 `{x}` 換成 `(<原本公式>)`
  - **套在欄位值外層**：把 `{x}` 換成 `COL('<欄位名>')`

範例：

- 你原本有欄位：
  - `btc_ret_1 = LOGRET(COL('BTCUSDT_klines_1m_close'), 1)`
- 模板：`ZSCORE({x})`，suffix：`__z`
- 選欄：`btc_ret_1`
- 結果會新增：
  - `btc_ret_1__z = ZSCORE(LOGRET(COL('BTCUSDT_klines_1m_close'), 1))`

---

### 5) 橫向（同一 timestamp 橫跨多欄）

選欄：

- **COLS("regex")**：用 regex 選一組欄（回傳欄集合）
  - 例：`COLS(".*_klines_1m_close$")`

Row-wise 聚合（把同一列多欄合成一欄）：

- **ROW_SUM(...)** / **ROW_MEAN(...)** / **ROW_MIN(...)** / **ROW_MAX(...)**

更多橫向統計（同一列多欄）：

- **ROW_STD(set)** / **ROW_VAR(set)**
- **ROW_MEDIAN(set)**
- **ROW_QUANTILE(set, q)**：q∈[0,1]
- **ROW_COUNT_VALID(set)**：非 null 的欄數
- **ROW_TOPK_MEAN(set, k)** / **ROW_BOTTOMK_MEAN(set, k)**

範例：

- **多標的 close 平均**：
  - `ROW_MEAN(COLS(".*_klines_1m_close$"))`
- **多標的 close 中挑最大**：
  - `ROW_MAX(COLS(".*_klines_1m_close$"))`
- **多標的 close 的 top-2 平均**：
  - `ROW_TOPK_MEAN(COLS(".*_klines_1m_close$"), 2)`
- **多標的 close 的 50% 分位（中位附近）**：
  - `ROW_QUANTILE(COLS(".*_klines_1m_close$"), 0.5)`

---

### 6) 正規化 / 裁切（Normalization）

> 這一包就是你說的「更多正規化方式」；其中 `ZSCORE/MINMAX/RANK_NORM` 都是全樣本（含 lookahead），若要因果請用 `ROLL_ZSCORE`。

- **ZSCORE(x)**：全樣本 zscore（含 lookahead）
- **ROLL_ZSCORE(x, w)**：rolling zscore（因果）
- **MINMAX(x)**：全樣本 min-max（含 lookahead）
- **RANK_NORM(x)**：全樣本 rank -> [0,1]（含 lookahead）
- **WINSORIZE(x, p_low, p_high)**：用分位裁切（全樣本）
- **ROBUST_Z(x)**：robust z（median/MAD）
- **CLIP/CLAMP(x, low, high)**：硬裁切

範例：

- `ROLL_ZSCORE(LOGRET(BTCUSDT_klines_1m_close, 1), 240)`
- `ZSCORE(WINSORIZE(LOGRET(BTCUSDT_klines_1m_close, 1), 0.01, 0.99))`
- `ROBUST_Z(BTCUSDT_klines_1m_volume)`

---

### 7) 跨標的正規化（Cross-sectional / 同 timestamp）

先用 `COLS("regex")` 選一組欄，然後對某個欄位 `x` 做同列的 demean/zscore/rank：

- **XS_DEMEAN(x, set)**：x - row_mean(set)
- **XS_ZSCORE(x, set)**：(x-row_mean)/row_std
- **XS_RANK(x, set)**：同列 rank（average rank）
- **XS_PCTRANK(x, set)**：同列百分位（0~1）
- **SOFTMAX_WEIGHT(x, set, temp)**：同列 softmax 權重

範例（以 close 在同列做 cross-sectional zscore）：

- `XS_ZSCORE(BTCUSDT_klines_1m_close, COLS(".*_klines_1m_close$"))`

---

### 8) 基礎函數（逐元素）

- `ABS(x)`
- `LOG(x)`
- `EXP(x)`
- `SQRT(x)`
- `SIGN(x)`
- `ROUND(x, digits)` / `FLOOR(x)` / `CEIL(x)`
- `CLIP(x, low, high)`
- `COALESCE(a, b)`：選第一個非空
- `MIN(a, b)` / `MAX(a, b)`（逐點）

---

### 9) 常用範例（直接貼進去就能跑）

- **BTC 1m logret**：
  - `LOGRET(BTCUSDT_klines_1m_close, 1)`
- **BTC 60 分鐘波動（rolling std of logret）**：
  - `ROLL_STD(LOGRET(BTCUSDT_klines_1m_close, 1), 60)`
- **OI 補值後再算變化**：
  - `DIFF(FILL_FFILL(BTCUSDT_metrics_sum_open_interest), 1)`
- **多標的 close 橫向平均（選出所有 close 欄）**：
  - `ROW_MEAN(COLS(".*_klines_1m_close$"))`
- **跨標的 zscore（BTC close 在同列 close set 裡的 z）**：
  - `XS_ZSCORE(BTCUSDT_klines_1m_close, COLS(".*_klines_1m_close$"))`


