"""
Schema Inspector：自動探勘資料結構
"""

import zipfile
import io
import requests
from typing import Dict, List, Optional, Any, Tuple
import polars as pl
from pathlib import Path
import re

from .registry import SchemaRegistry
from ..utils.file_utils import ensure_dir


class SchemaInspector:
    """Schema 探勘器"""
    
    # 已知的官方 schema（來自 Binance public data 說明）
    KNOWN_SCHEMAS = {
        # 注意：Binance Vision 部分檔案早期可能「沒有表頭」，後期才有表頭。
        # 這裡用「檔案實際常見表頭」作為 canonical 名稱，讓我們能在無表頭時套用正確欄位名。
        "klines": [
            {"name": "open_time", "dtype": "int64", "position": 0},
            {"name": "open", "dtype": "float64", "position": 1},
            {"name": "high", "dtype": "float64", "position": 2},
            {"name": "low", "dtype": "float64", "position": 3},
            {"name": "close", "dtype": "float64", "position": 4},
            {"name": "volume", "dtype": "float64", "position": 5},
            {"name": "close_time", "dtype": "int64", "position": 6},
            {"name": "quote_volume", "dtype": "float64", "position": 7},
            {"name": "count", "dtype": "int64", "position": 8},
            {"name": "taker_buy_volume", "dtype": "float64", "position": 9},
            {"name": "taker_buy_quote_volume", "dtype": "float64", "position": 10},
            {"name": "ignore", "dtype": "float64", "position": 11},
        ],
        # klines-like（futures 的 mark/index/premium 檔案實務上欄位結構通常同 klines）
        "markPriceKlines": [
            {"name": "open_time", "dtype": "int64", "position": 0},
            {"name": "open", "dtype": "float64", "position": 1},
            {"name": "high", "dtype": "float64", "position": 2},
            {"name": "low", "dtype": "float64", "position": 3},
            {"name": "close", "dtype": "float64", "position": 4},
            {"name": "volume", "dtype": "float64", "position": 5},
            {"name": "close_time", "dtype": "int64", "position": 6},
            {"name": "quote_volume", "dtype": "float64", "position": 7},
            {"name": "count", "dtype": "int64", "position": 8},
            {"name": "taker_buy_volume", "dtype": "float64", "position": 9},
            {"name": "taker_buy_quote_volume", "dtype": "float64", "position": 10},
            {"name": "ignore", "dtype": "float64", "position": 11},
        ],
        "indexPriceKlines": [
            {"name": "open_time", "dtype": "int64", "position": 0},
            {"name": "open", "dtype": "float64", "position": 1},
            {"name": "high", "dtype": "float64", "position": 2},
            {"name": "low", "dtype": "float64", "position": 3},
            {"name": "close", "dtype": "float64", "position": 4},
            {"name": "volume", "dtype": "float64", "position": 5},
            {"name": "close_time", "dtype": "int64", "position": 6},
            {"name": "quote_volume", "dtype": "float64", "position": 7},
            {"name": "count", "dtype": "int64", "position": 8},
            {"name": "taker_buy_volume", "dtype": "float64", "position": 9},
            {"name": "taker_buy_quote_volume", "dtype": "float64", "position": 10},
            {"name": "ignore", "dtype": "float64", "position": 11},
        ],
        "premiumIndexKlines": [
            {"name": "open_time", "dtype": "int64", "position": 0},
            {"name": "open", "dtype": "float64", "position": 1},
            {"name": "high", "dtype": "float64", "position": 2},
            {"name": "low", "dtype": "float64", "position": 3},
            {"name": "close", "dtype": "float64", "position": 4},
            {"name": "volume", "dtype": "float64", "position": 5},
            {"name": "close_time", "dtype": "int64", "position": 6},
            {"name": "quote_volume", "dtype": "float64", "position": 7},
            {"name": "count", "dtype": "int64", "position": 8},
            {"name": "taker_buy_volume", "dtype": "float64", "position": 9},
            {"name": "taker_buy_quote_volume", "dtype": "float64", "position": 10},
            {"name": "ignore", "dtype": "float64", "position": 11},
        ],
        # futures funding rate history (Vision monthly/daily fundingRate)
        # sample file contains header:
        # calc_time,funding_interval_hours,last_funding_rate
        "fundingRate": [
            {"name": "calc_time", "dtype": "int64", "position": 0},
            {"name": "funding_interval_hours", "dtype": "int64", "position": 1},
            {"name": "last_funding_rate", "dtype": "float64", "position": 2},
        ],
        "aggTrades": [
            {"name": "agg_trade_id", "dtype": "int64", "position": 0},
            {"name": "price", "dtype": "float64", "position": 1},
            {"name": "quantity", "dtype": "float64", "position": 2},
            {"name": "first_trade_id", "dtype": "int64", "position": 3},
            {"name": "last_trade_id", "dtype": "int64", "position": 4},
            {"name": "transact_time", "dtype": "int64", "position": 5},
            {"name": "is_buyer_maker", "dtype": "bool", "position": 6},
        ],
        "trades": [
            {"name": "id", "dtype": "int64", "position": 0},
            {"name": "price", "dtype": "float64", "position": 1},
            {"name": "qty", "dtype": "float64", "position": 2},
            {"name": "quote_qty", "dtype": "float64", "position": 3},
            {"name": "time", "dtype": "int64", "position": 4},
            {"name": "is_buyer_maker", "dtype": "bool", "position": 5},
        ],
        "bookTicker": [
            {"name": "update_id", "dtype": "int64", "position": 0},
            {"name": "best_bid_price", "dtype": "float64", "position": 1},
            {"name": "best_bid_qty", "dtype": "float64", "position": 2},
            {"name": "best_ask_price", "dtype": "float64", "position": 3},
            {"name": "best_ask_qty", "dtype": "float64", "position": 4},
            {"name": "transaction_time", "dtype": "int64", "position": 5},
            {"name": "event_time", "dtype": "int64", "position": 6},
        ],
        "bookDepth": [
            {"name": "timestamp", "dtype": "int64", "position": 0},
            {"name": "percentage", "dtype": "float64", "position": 1},
            {"name": "depth", "dtype": "float64", "position": 2},
            {"name": "notional", "dtype": "float64", "position": 3},
        ],
    }
    
    def __init__(self, schema_registry: SchemaRegistry, cache_dir: str = "cache/samples"):
        self.schema_registry = schema_registry
        self.cache_dir = cache_dir
        ensure_dir(cache_dir)
        self.session = requests.Session()
    
    def inspect_dataset(self, dataset_type: str, sample_urls: List[str],
                      symbol: str = "BTCUSDT") -> Dict[str, Any]:
        """
        探勘資料集結構
        
        Args:
            dataset_type: 資料集類型
            sample_urls: 樣本檔案 URL 列表
            symbol: 交易對（用於記錄）
        
        Returns:
            schema 資訊
        """
        all_columns = []
        all_dtypes = {}
        first_n_rows_list = []
        
        for url in sample_urls:
            try:
                # 下載並解析
                result = self._download_and_parse(url, dataset_type)
                if result:
                    columns, dtypes, first_n_rows = result
                    if not all_columns:
                        all_columns = columns
                        all_dtypes = dtypes
                    first_n_rows_list.extend(first_n_rows[:5])  # 每檔取前 5 行
            except Exception as e:
                print(f"Warning: Failed to inspect {url}: {e}")
                continue
        
        if not all_columns:
            raise ValueError(f"Failed to extract schema from any sample for {dataset_type}")
        
        # 推斷 primary_time_key 和 join_key
        primary_time_key = self._infer_time_key(all_columns)
        join_key = primary_time_key
        
        # 與已知 schema 比較
        expected_schema = self.KNOWN_SCHEMAS.get(dataset_type)
        validation_status = "valid" if expected_schema else "unknown"
        
        if expected_schema:
            # 驗證：檢查欄位數量與名稱（按順序）
            expected_names = [c["name"] for c in expected_schema]
            actual_names = [c["name"] for c in all_columns]
            if expected_names != actual_names:
                validation_status = "invalid"
        
        # 建立欄位註解（特別標記 ignore 欄位）
        field_notes = {}
        for col in all_columns:
            if "ignore" in col["name"].lower():
                field_notes[col["name"]] = "通常為 0 或忽略欄位"
        
        # 註冊 schema
        schema = self.schema_registry.register_schema(
            dataset_type=dataset_type,
            columns=all_columns,
            dtypes=all_dtypes,
            primary_time_key=primary_time_key,
            join_key=join_key,
            field_notes=field_notes,
            expected_schema=expected_schema,
            validation_status=validation_status
        )
        
        # 記錄樣本
        for url in sample_urls[:3]:  # 最多記錄 3 個樣本
            try:
                sample_date = self._extract_sample_date(url)
                self.schema_registry.add_sample(
                    dataset_type=dataset_type,
                    symbol=symbol,
                    sample_file_url=url,
                    sample_date=sample_date,
                    first_n_rows=first_n_rows_list[:10],
                    row_count=None,
                    file_size=None
                )
            except:
                pass
        
        return {
            "dataset_type": dataset_type,
            "columns": all_columns,
            "dtypes": all_dtypes,
            "primary_time_key": primary_time_key,
            "join_key": join_key,
            "field_notes": field_notes,
            "validation_status": validation_status
        }

    def _extract_sample_date(self, url: str) -> str:
        """
        從 Vision 檔名末尾抓日期：
        - daily:   ...-YYYY-MM-DD.zip
        - monthly: ...-YYYY-MM.zip
        - interval monthly: ...-YYYY-MM.zip（例如 klines 類）
        """
        u = (url or "").strip()
        m = re.search(r"-(\d{4}-\d{2}-\d{2}|\d{4}-\d{2})\.(zip|csv)$", u)
        if m:
            return m.group(1)
        return "unknown"
    
    def _download_and_parse(self, url: str, dataset_type: str) -> Optional[tuple]:
        """下載並解析檔案"""
        try:
            response = self.session.get(url, timeout=30, stream=True)
            response.raise_for_status()
            
            # 讀取 zip
            zip_data = io.BytesIO(response.content)
            with zipfile.ZipFile(zip_data, 'r') as zf:
                # 找 CSV 檔案
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                if not csv_files:
                    return None
                
                # 讀取第一個 CSV（通常只有一個）
                csv_content = zf.read(csv_files[0])

                # 偵測是否有表頭（早期很多檔案沒有表頭）
                expected_schema = self.KNOWN_SCHEMAS.get(dataset_type)
                expected_names = [c["name"] for c in expected_schema] if expected_schema else None
                has_header = self._detect_header(csv_content, expected_names=expected_names)

                # 用 polars 讀取（只讀前幾行來推斷 schema）
                if has_header:
                    df = pl.read_csv(io.BytesIO(csv_content), n_rows=100, infer_schema_length=200)
                else:
                    # 無表頭：若有已知 schema 且欄數一致，套用欄位名；否則用 col_0..col_n
                    ncols = self._infer_ncols_from_first_line(csv_content)
                    if expected_names and len(expected_names) == ncols:
                        new_cols = expected_names
                    else:
                        new_cols = [f"col_{i}" for i in range(ncols)]
                    df = pl.read_csv(
                        io.BytesIO(csv_content),
                        n_rows=100,
                        has_header=False,
                        new_columns=new_cols,
                        infer_schema_length=200,
                    )
                
                # 提取欄位資訊
                columns = [
                    {"name": col, "dtype": str(df[col].dtype), "position": i}
                    for i, col in enumerate(df.columns)
                ]
                
                dtypes = {col: str(df[col].dtype) for col in df.columns}
                
                # 轉換前幾行為字典
                first_n_rows = df.head(10).to_dicts()

                # 若是無表頭但我們套用了 known schema，補上註記
                if not has_header and expected_names and df.columns == expected_names:
                    for c in columns:
                        c["inferred_header"] = True
                
                return columns, dtypes, first_n_rows
                
        except Exception as e:
            print(f"Error parsing {url}: {e}")
            return None
    
    def _infer_time_key(self, columns: List[Dict]) -> Optional[str]:
        """推斷時間鍵欄位名稱"""
        time_key_candidates = ["open_time", "close_time", "event_time", "timestamp", "time"]
        for col in columns:
            if col["name"] in time_key_candidates:
                return col["name"]
        return None

    def _detect_header(self, csv_content: bytes, expected_names: Optional[List[str]] = None) -> bool:
        """
        用第一行判斷是否為 header。
        規則：若有明顯字母/底線等，視為 header；否則視為資料行。
        """
        first_line = csv_content.splitlines()[0] if csv_content else b""
        try:
            s = first_line.decode("utf-8", errors="ignore").strip()
        except Exception:
            s = str(first_line)

        # 空行保守當成有 header（讓 polars 自己處理）
        if not s:
            return True

        tokens = [t.strip() for t in s.split(",")]
        return self._looks_like_header(tokens, expected_names=expected_names)

    def _looks_like_header(self, tokens: List[str], expected_names: Optional[List[str]] = None) -> bool:
        """
        更穩健的表頭判斷：
        - 若有 expected_names（已知 schema），優先用「是否像欄位名集合」判斷
        - 否則用通用規則：多數 token 是 identifier-like 才視為 header
        """
        if not tokens:
            return True

        # 1) 有已知 schema 時：如果 tokens 大量命中 expected_names，幾乎可確定是 header
        if expected_names:
            exp = set(expected_names)
            hit = sum(1 for t in tokens if t in exp)
            # 只要命中一半以上，或完全相等，就當 header
            if hit >= max(1, len(tokens) // 2):
                return True
            # 反過來：若幾乎沒命中，偏向無表頭
            # （避免像 aggTrades/trades 的 data row 出現 true/false 被誤判）
            return False

        # 2) 無已知 schema：通用 heuristic
        id_like = 0
        val_like = 0
        for t in tokens:
            if self._is_bool_literal(t) or self._is_number(t):
                val_like += 1
            else:
                # 欄位名通常包含字母或底線，且不會像數字
                if any(ch.isalpha() for ch in t) or "_" in t:
                    id_like += 1
                else:
                    val_like += 1

        # 多數是 identifier-like 才當 header
        return id_like > val_like

    def _is_bool_literal(self, t: str) -> bool:
        x = t.strip().lower()
        return x in ("true", "false")

    def _is_number(self, t: str) -> bool:
        x = t.strip()
        if x == "":
            return False
        try:
            float(x)
            return True
        except Exception:
            return False

    def _infer_ncols_from_first_line(self, csv_content: bytes) -> int:
        """從第一行推斷欄數（逗號分隔）"""
        first_line = csv_content.splitlines()[0] if csv_content else b""
        try:
            s = first_line.decode("utf-8", errors="ignore").strip()
        except Exception:
            s = ""
        if not s:
            return 0
        return len(s.split(","))

