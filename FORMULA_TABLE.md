# Table Paster 公式參考

`Table Paster` 用於已完成時間對齊的寬表，定位是二維資料處理層。所有公式皆以欄為單位運算，底層會編譯為 Polars Expr，並在 LazyFrame 上執行。

這套公式系統同時支援兩個方向：

- 縱向運算：沿時間軸處理單一欄位
- 橫向運算：在同一個 timestamp 下跨多欄位計算

這個設計的目的有兩個：

- 保持公式語法接近試算表與研究流程慣用寫法
- 避免任意 Python 執行，將計算限制在可分析、可向量化的範圍內

## 設計原則

### 向量化

每個公式輸入的是一個欄位或一組欄位，輸出仍然是一個欄位。沒有逐列 Python 迴圈，也不支援任意副作用。

### 安全性

公式會先經過 AST 檢查，只允許：

- 常數
- 欄位名稱
- 算術與比較運算
- 布林運算
- 函數呼叫

不允許：

- 屬性存取
- 下標索引
- lambda
- 任意 Python 內建

### 依賴排序

當一次建立多個新欄位時，系統會自動解析公式依賴，先建立前置欄位，再建立後續欄位。公式之間因此可以像工作表一樣串接，無需手動調整順序。

## 語法規則

### 欄位引用

欄位名稱若本身是合法識別字，可直接使用：

- `BTCUSDT_klines_1m_close`

欄位名稱較特殊，或需要更明確的寫法時，可使用：

- `COL("BTCUSDT_klines_1m_close")`

### 欄位集合

欄位集合可透過兩種方式取得：

- `COLS("regex")`：以正則表達式選欄
- `SET(a, b, c, ...)`：手動指定欄位集合

例如：

- `COLS(".*_klines_1m_close$")`
- `SET(BTCUSDT_klines_1m_close, ETHUSDT_klines_1m_close)`

### 運算子

支援的運算子如下：

- 算術：`+ - * / ** %`
- 比較：`> >= < <= == !=`
- 邏輯：`AND OR NOT`
- 小寫寫法同樣可用：`and or not`

## 函數總覽

### 欄位與選欄

| 類別 | 函數 | 說明 |
| --- | --- | --- |
| 欄位 | `COL(name)` | 以字串引用欄位 |
| 欄位集合 | `COLS(pattern)` | 以 regex 選取欄位集合 |
| 欄位集合 | `SET(...)` | 手動建立欄位集合 |

### 時間序列函數

| 函數 | 參數 | 說明 |
| --- | --- | --- |
| `LAG(x, n)` | `x, n` | 位移 `n` 期 |
| `DIFF(x, n)` | `x, n` | `x - lag(x, n)` |
| `PCT_CHANGE(x, n)` | `x, n` | 簡單報酬率 |
| `LOGRET(x, n)` | `x, n` | 對數報酬率 |
| `CUMSUM(x)` | `x` | 累積和 |
| `CUMPROD(x)` | `x` | 累積乘積 |

### Rolling 與平滑

| 函數 | 參數 | 說明 |
| --- | --- | --- |
| `ROLL_MEAN(x, w)` | `x, w` | rolling 平均 |
| `ROLL_STD(x, w)` | `x, w` | rolling 標準差 |
| `ROLL_SUM(x, w)` | `x, w` | rolling 總和 |
| `ROLL_ZSCORE(x, w)` | `x, w` | rolling z-score |
| `EMA(x, span)` | `x, span` | 指數移動平均 |

### 缺值與資料品質

| 函數 | 參數 | 說明 |
| --- | --- | --- |
| `FILL_FFILL(x)` | `x` | 前向補值 |
| `FILL_BFILL(x)` | `x` | 後向補值 |
| `FILL_ZERO(x)` | `x` | 空值補 0 |
| `ISNA(x)` | `x` | 是否為 null |
| `ISFINITE(x)` | `x` | 是否為有限值 |
| `COALESCE(a, b)` | `a, b` | 取第一個非空值 |

### 逐元素函數

| 函數 | 參數 | 說明 |
| --- | --- | --- |
| `ABS(x)` | `x` | 絕對值 |
| `LOG(x)` | `x` | 自然對數 |
| `EXP(x)` | `x` | 指數函數 |
| `SQRT(x)` | `x` | 平方根 |
| `SIGN(x)` | `x` | 符號函數 |
| `ROUND(x, digits)` | `x, digits` | 四捨五入 |
| `FLOOR(x)` | `x` | 向下取整 |
| `CEIL(x)` | `x` | 向上取整 |
| `CLIP(x, low, high)` | `x, low, high` | 區間裁切 |
| `CLAMP(x, low, high)` | `x, low, high` | 與 `CLIP` 等價 |
| `MIN(a, b)` | `a, b` | 逐點最小值 |
| `MAX(a, b)` | `a, b` | 逐點最大值 |
| `IF(cond, a, b)` | `cond, a, b` | 向量化條件判斷 |

### 正規化與裁切

| 函數 | 參數 | 說明 |
| --- | --- | --- |
| `ZSCORE(x)` | `x` | 全樣本 z-score |
| `MINMAX(x)` | `x` | 全樣本 min-max normalization |
| `RANK_NORM(x)` | `x` | 全樣本 rank normalization |
| `WINSORIZE(x, p_low, p_high)` | `x, p_low, p_high` | 以分位數裁切極端值 |
| `ROBUST_Z(x)` | `x` | 以 median / MAD 計算 robust z-score |

### 橫向統計

| 函數 | 參數 | 說明 |
| --- | --- | --- |
| `ROW_SUM(...)` | 欄位集合 | 同列加總 |
| `ROW_MEAN(...)` | 欄位集合 | 同列平均 |
| `ROW_MIN(...)` | 欄位集合 | 同列最小值 |
| `ROW_MAX(...)` | 欄位集合 | 同列最大值 |
| `ROW_STD(...)` | 欄位集合 | 同列標準差 |
| `ROW_VAR(...)` | 欄位集合 | 同列變異數 |
| `ROW_MEDIAN(...)` | 欄位集合 | 同列中位數 |
| `ROW_QUANTILE(set, q)` | 集合, 分位數 | 同列分位數 |
| `ROW_COUNT_VALID(set)` | 集合 | 同列非空欄位數 |
| `ROW_TOPK_MEAN(set, k)` | 集合, k | 同列前 k 大平均 |
| `ROW_BOTTOMK_MEAN(set, k)` | 集合, k | 同列前 k 小平均 |

### Cross-sectional 函數

| 函數 | 參數 | 說明 |
| --- | --- | --- |
| `XS_DEMEAN(x, set)` | `x, set` | 同列去均值 |
| `XS_ZSCORE(x, set)` | `x, set` | 同列 z-score |
| `XS_RANK(x, set)` | `x, set` | 同列平均名次 |
| `XS_PCTRANK(x, set)` | `x, set` | 同列百分位名次 |
| `SOFTMAX_WEIGHT(x, set, temp)` | `x, set, temp` | 同列 softmax 權重 |

## lookahead 說明

以下函數使用全樣本統計，若直接用於嚴格的時間序列建模，會帶入 lookahead：

- `ZSCORE`
- `MINMAX`
- `RANK_NORM`
- `WINSORIZE`
- `ROBUST_Z`

若目標偏向因果特徵，可優先使用：

- `LAG`
- `DIFF`
- `PCT_CHANGE`
- `LOGRET`
- `ROLL_*`
- `FILL_FFILL`

## 參數與行為細節

### `LAG / DIFF / PCT_CHANGE / LOGRET`

- `n` 必須為整數
- `n=1` 代表與前一期比較
- 首 `n` 期通常會得到空值

### `ROLL_*`

- `w` 為窗口大小
- 目前 `min_periods = w`
- 換句話說，窗口未滿之前不會強行輸出值

### `EMA`

- `span` 為整數
- 底層使用 Polars 的 `ewm_mean`

### `WINSORIZE`

- `p_low`、`p_high` 必須落在 `[0, 1]`
- 並且需滿足 `p_low <= p_high`

### `ROW_QUANTILE`

- `q` 必須落在 `[0, 1]`
- 實作上會先排序，再取對應分位位置

### `SOFTMAX_WEIGHT`

- `temp` 必須大於 `0`
- 溫度愈小，權重會愈集中

## 範例

### 報酬與波動

```text
LOGRET(BTCUSDT_klines_1m_close, 1)
ROLL_STD(LOGRET(BTCUSDT_klines_1m_close, 1), 60)
ROLL_ZSCORE(LOGRET(BTCUSDT_klines_1m_close, 1), 240)
EMA(BTCUSDT_klines_1m_close, 60)
```

### OI / funding / metrics 補值後特徵

```text
FILL_FFILL(BTCUSDT_metrics_sum_open_interest)
DIFF(FILL_FFILL(BTCUSDT_metrics_sum_open_interest), 1)
LOGRET(FILL_FFILL(BTCUSDT_metrics_sum_open_interest), 1)
```

### 多標的橫向特徵

```text
ROW_MEAN(COLS(".*_klines_1m_close$"))
ROW_STD(COLS(".*_klines_1m_close$"))
ROW_TOPK_MEAN(COLS(".*_klines_1m_close$"), 3)
ROW_COUNT_VALID(COLS(".*_klines_1m_close$"))
```

### Cross-sectional 正規化

```text
XS_ZSCORE(BTCUSDT_klines_1m_close, COLS(".*_klines_1m_close$"))
XS_PCTRANK(BTCUSDT_klines_1m_volume, COLS(".*_klines_1m_volume$"))
SOFTMAX_WEIGHT(BTCUSDT_klines_1m_close, COLS(".*_klines_1m_close$"), 1.0)
```

### 裁切與正規化

```text
WINSORIZE(LOGRET(BTCUSDT_klines_1m_close, 1), 0.01, 0.99)
ZSCORE(WINSORIZE(LOGRET(BTCUSDT_klines_1m_close, 1), 0.01, 0.99))
ROBUST_Z(BTCUSDT_klines_1m_volume)
MINMAX(BTCUSDT_klines_1m_close)
```

### 條件與逐元素轉換

```text
IF(BTCUSDT_klines_1m_volume > 0, BTCUSDT_klines_1m_close, 0)
ABS(LOGRET(BTCUSDT_klines_1m_close, 1))
CLIP(BTCUSDT_klines_1m_volume, 0, 1000000)
COALESCE(BTCUSDT_metrics_sum_open_interest, 0)
```

## 全欄位模板

全欄位模板用於將同一種轉換批次套用到多個欄位。模板必須包含 `{x}`，代表當前欄位或既有公式。

常見用法：

- 先建立基礎特徵
- 再批次套上標準化、裁切或平滑

例如：

- 原公式：`btc_ret_1 = LOGRET(COL("BTCUSDT_klines_1m_close"), 1)`
- 模板：`ZSCORE({x})`
- suffix：`__z`

會得到：

- `btc_ret_1__z = ZSCORE(LOGRET(COL("BTCUSDT_klines_1m_close"), 1))`

## 使用原則

### 先建立低階欄位，再建立高階欄位

較穩定的順序通常是：

1. 原始欄位
2. 報酬率 / 差分 / 補值
3. rolling 統計
4. 正規化
5. 橫向或 cross-sectional 特徵

### 先確認欄位語意，再套標準化

原欄位的時間對齊邏輯若尚未確認，過早套用 `ZSCORE` 或 `RANK_NORM` 容易掩蓋資料問題。

### 先小規模驗證

第一次建立新公式時，通常先做以下檢查：

- 只做 1 到 3 條公式
- 先檢查欄位值是否合理
- 確認空值分布與尺度
- 再批次擴大


