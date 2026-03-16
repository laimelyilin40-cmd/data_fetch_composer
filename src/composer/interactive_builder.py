"""
互動式 Dataset Builder（給 UI 用）

功能：
- 依 selection（market/symbol/dataset/interval/columns）下載 Vision 檔案（daily）
- 依指定 anchor（主軸）以 ts 對齊合併
- 缺值用「相鄰兩值平均」（線性插值）補值，並輸出報告
- 輸出 Parquet + manifest/report（方便 ML/回測載入）
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
import zipfile
import io

import polars as pl
from sqlalchemy.exc import OperationalError

from ..catalog.database import CatalogDB
from ..catalog.builder import CatalogBuilder
from ..downloader.client import DownloadClient
from ..schema.registry import SchemaRegistry
from ..cache.raw_cache import RawParquetCache, INTERVAL_DATASETS as CACHE_INTERVAL_DATASETS


INTERVAL_DATASETS = {"klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"}


@dataclass
class Selection:
    market: str  # "spot" / "futures_um"
    symbol: str  # e.g. "ETHUSDT"
    dataset_type: str  # e.g. "klines"
    cadence: str = "daily"  # daily/monthly (目前 builder 先做 daily)
    interval: Optional[str] = None
    use_all_columns: bool = True
    columns: Optional[List[str]] = None

    # optional override
    time_key_override: Optional[str] = None


@dataclass
class BuildConfig:
    name: str
    start: str  # YYYY-MM-DD
    end: str  # YYYY-MM-DD
    anchor_index: int = 0
    output_dir: str = "data/outputs/datasets"
    download_dir: str = "data/downloads"
    fill_strategy: str = "adjacent_avg"  # 線性插值（相鄰平均）


class InteractiveDatasetBuilder:
    def __init__(self, catalog_db_path: str = "catalog.db", schema_db_path: str = "schema.db"):
        self.catalog_db = CatalogDB(catalog_db_path)
        # 觸發 migration/backfill；若 SQLite 正被其他程序寫入，先略過避免 UI 中斷。
        try:
            self.catalog_db.init_database()
        except OperationalError as e:
            if "database is locked" not in str(e).lower():
                raise
        self.schema_registry = SchemaRegistry(schema_db_path)
        self.downloader = DownloadClient(download_dir="data/downloads")
        self.cache = RawParquetCache(cache_root="data/raw_parquet", zip_root="data/raw_zips", schema_db_path=schema_db_path, threads=32)

    def build(self, selections: List[Selection], cfg: BuildConfig) -> Tuple[str, str]:
        """
        Returns:
            (parquet_path, report_path)
        """
        if not selections:
            raise ValueError("selections is empty")
        if cfg.anchor_index < 0 or cfg.anchor_index >= len(selections):
            raise ValueError("anchor_index out of range")

        out_root = Path(cfg.output_dir) / cfg.name
        out_root.mkdir(parents=True, exist_ok=True)

        # 1) 下載 + 讀取每個 selection 成 DataFrame
        dfs: List[pl.DataFrame] = []
        per_sel_report: List[Dict[str, Any]] = []
        for sel in selections:
            df, rep = self._load_selection_cached(sel, cfg.start, cfg.end)
            dfs.append(df)
            per_sel_report.append(rep)

        total_rows = sum(df.height for df in dfs)
        if total_rows == 0:
            raise ValueError(
                "找不到可用檔案（all selections task_count=0）。"
                "請先重建 catalog 檔案索引：執行 `python setup_catalog.py` "
                "或 `python -m cli.main catalog-build ...`，再重試 Dataset Builder。"
            )

        # 2) 以 anchor 的 ts 當主軸
        anchor_df = dfs[cfg.anchor_index]
        if "ts" not in anchor_df.columns:
            raise ValueError("Anchor dataframe missing ts column")
        result = anchor_df

        for i, df in enumerate(dfs):
            if i == cfg.anchor_index:
                continue
            if "ts" not in df.columns:
                raise ValueError(f"Selection {i} missing ts column")
            # 避免重複 ts 欄
            other_cols = [c for c in df.columns if c != "ts"]
            result = result.join(df.select(["ts"] + other_cols), on="ts", how="left")

        # 3) 缺值補值（線性插值 = 相鄰平均）
        fill_report = self._fill_missing(result, cfg.fill_strategy)
        result = fill_report["df"]
        fill_report.pop("df", None)

        # 4) 儲存
        parquet_path = out_root / f"{cfg.name}.parquet"
        result.write_parquet(parquet_path)

        # 5) report / manifest
        report = {
            "name": cfg.name,
            "generated_at": datetime.utcnow().isoformat(),
            "config": asdict(cfg),
            "selections": [asdict(s) for s in selections],
            "per_selection": per_sel_report,
            "fill_report": fill_report,
            "output": {
                "parquet": str(parquet_path),
                "columns": result.columns,
                "row_count": result.height,
            },
        }
        report_path = out_root / f"{cfg.name}.report.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        recipe_path = out_root / f"{cfg.name}.recipe.json"
        with open(recipe_path, "w", encoding="utf-8") as f:
            json.dump({"config": asdict(cfg), "selections": [asdict(s) for s in selections]}, f, ensure_ascii=False, indent=2)

        return str(parquet_path), str(report_path)

    # ---------------- internals ----------------
    def _load_selection_cached(self, sel: Selection, start: str, end: str) -> Tuple[pl.DataFrame, Dict[str, Any]]:
        schema = self.schema_registry.get_schema(sel.dataset_type)

        # columns to use
        if sel.use_all_columns:
            cols = self._all_columns_excluding_ignore(schema)
        else:
            cols = sel.columns or []

        # ensure time key is present
        time_key = sel.time_key_override or self._infer_time_key(schema, cols)
        if time_key and time_key not in cols:
            cols = [time_key] + cols

        selected_cadence = (sel.cadence or "daily").lower()
        prefer_monthly = selected_cadence == "monthly"

        # 1) build raw cache for this selection
        tasks, plan_meta = self.cache.plan_tasks(
            market=sel.market,
            dataset_type=sel.dataset_type,
            symbol=sel.symbol,
            start=start,
            end=end,
            interval=sel.interval if sel.dataset_type in CACHE_INTERVAL_DATASETS else None,
            prefer_monthly=prefer_monthly,
        )
        manifest = self.cache.build_cache(
            tasks,
            prefer_monthly=prefer_monthly,
            manifest_name=f"cache_{sel.market}_{sel.symbol}_{sel.dataset_type}_{selected_cadence}",
        )

        # 2) scan cached parquet with selected cadence
        lf_daily = None
        lf_monthly = None
        if selected_cadence == "daily":
            lf_daily = self.cache.scan_cached(
                market=sel.market,
                dataset_type=sel.dataset_type,
                symbol=sel.symbol,
                start=start,
                end=end,
                interval=sel.interval if sel.dataset_type in CACHE_INTERVAL_DATASETS else None,
                cadence="daily",
                columns=None,
            )
        elif selected_cadence == "monthly":
            lf_monthly = self.cache.scan_cached(
                market=sel.market,
                dataset_type=sel.dataset_type,
                symbol=sel.symbol,
                start=start,
                end=end,
                interval=sel.interval if sel.dataset_type in CACHE_INTERVAL_DATASETS else None,
                cadence="monthly",
                columns=None,
            )
        else:
            # fallback: unknown cadence, keep original behavior
            lf_daily = self.cache.scan_cached(
                market=sel.market,
                dataset_type=sel.dataset_type,
                symbol=sel.symbol,
                start=start,
                end=end,
                interval=sel.interval if sel.dataset_type in CACHE_INTERVAL_DATASETS else None,
                cadence="daily",
                columns=None,
            )
            lf_monthly = self.cache.scan_cached(
                market=sel.market,
                dataset_type=sel.dataset_type,
                symbol=sel.symbol,
                start=start,
                end=end,
                interval=sel.interval if sel.dataset_type in CACHE_INTERVAL_DATASETS else None,
                cadence="monthly",
                columns=None,
            )

        dfs_scan: List[pl.DataFrame] = []
        try:
            dfd = lf_daily.collect() if lf_daily is not None else pl.DataFrame()
            if dfd.height > 0:
                dfs_scan.append(dfd)
        except Exception:
            pass
        try:
            dfm = lf_monthly.collect() if lf_monthly is not None else pl.DataFrame()
            if dfm.height > 0:
                dfs_scan.append(dfm)
        except Exception:
            pass

        if not dfs_scan:
            df = pl.DataFrame()
        elif len(dfs_scan) == 1:
            df = dfs_scan[0]
        else:
            # daily 優先：先放 daily，再放 monthly；再用 (ts + 欄位) 去重（keep first）
            df = pl.concat(dfs_scan, how="diagonal_relaxed")

        # normalize time key to ts
        # If file already has open_time/time/etc, keep; else use provenance _date_key is not enough.
        df = self._normalize_time(df, time_key)

        # 篩選時間範圍（避免 monthly 包含整月資料）
        try:
            start_ms = int(datetime.strptime(start, "%Y-%m-%d").timestamp() * 1000)
            end_ms = int(datetime.strptime(end, "%Y-%m-%d").timestamp() * 1000) + 24 * 3600 * 1000 - 1
            if "ts" in df.columns:
                df = df.filter((pl.col("ts") >= start_ms) & (pl.col("ts") <= end_ms))
        except Exception:
            pass

        # select cols
        keep_cols = ["ts"] + [c for c in cols if c and c != time_key and c in df.columns]
        df = df.select([c for c in keep_cols if c in df.columns])

        # de-duplicate on ts: keep last
        if "ts" in df.columns:
            df = df.sort("ts").unique(subset=["ts"], keep="last")

        # prefix columns (except ts)
        prefix = self._make_prefix(sel)
        rename_map = {c: f"{prefix}{c}" for c in df.columns if c != "ts"}
        df = df.rename(rename_map)

        report = {
            "selection": asdict(sel),
            "selected_cadence": selected_cadence,
            "time_key": time_key,
            "cache_plan": plan_meta,
            "cache_manifest": {
                "manifest_path": manifest.get("manifest_path"),
                "summary": manifest.get("summary"),
                # 注意：missing_preview 會被截斷（預覽用），總數請看 summary.missing
                "missing_total": int((manifest.get("summary") or {}).get("missing", 0)),
                "error_total": int((manifest.get("summary") or {}).get("error", 0)),
                "skipped_total": int((manifest.get("summary") or {}).get("skipped", 0)),
                "ok_total": int((manifest.get("summary") or {}).get("ok", 0)),
                "missing_preview_count": len(manifest.get("missing_preview", [])),
                "error_preview_count": len(manifest.get("error_preview", [])),
            },
            "columns_selected": [c for c in cols if c and c != time_key],
            "final_columns": df.columns,
        }
        return df, report

    def _make_prefix(self, sel: Selection) -> str:
        # e.g. ETHUSDT_klines_1h_open
        itv = f"{sel.interval}_" if (sel.dataset_type in INTERVAL_DATASETS and sel.interval) else ""
        return f"{sel.symbol}_{sel.dataset_type}_{itv}"

    def _all_columns_excluding_ignore(self, schema: Optional[Dict[str, Any]]) -> List[str]:
        if not schema:
            return []
        cols = [c["name"] for c in schema.get("columns", [])]
        return [c for c in cols if c.lower() != "ignore"]

    def _infer_time_key(self, schema: Optional[Dict[str, Any]], cols: List[str]) -> Optional[str]:
        # 1) schema primary
        if schema and schema.get("primary_time_key"):
            return schema["primary_time_key"]
        # 2) common candidates (優先 open_time)
        for c in ["open_time", "create_time", "time", "timestamp", "event_time", "transact_time"]:
            if c in cols:
                return c
        # 3) fallback: first column if any
        return cols[0] if cols else None

    def _normalize_time(self, df: pl.DataFrame, time_key: Optional[str]) -> pl.DataFrame:
        if not time_key or time_key not in df.columns:
            # no time: create empty ts
            return df.with_columns(pl.lit(None).cast(pl.Int64).alias("ts"))

        # normalize to unix timestamp in milliseconds (Int64)
        col = pl.col(time_key)
        out = df

        if out[time_key].dtype == pl.Utf8:
            # string time: try parse datetime formats; fallback to int
            dt = pl.coalesce(
                [
                    col.str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f", strict=False),
                    col.str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False),
                    col.str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False),
                ]
            )
            ts_from_dt = dt.dt.epoch(time_unit="ms").cast(pl.Int64, strict=False)
            ts_from_int = col.cast(pl.Int64, strict=False)
            out = out.with_columns(pl.coalesce([ts_from_dt, ts_from_int]).alias("ts"))
        else:
            out = out.with_columns(col.cast(pl.Int64, strict=False).alias("ts"))

        # fix unit: seconds/us/ns -> ms
        # - seconds around 1e9
        # - ms around 1e12
        # - us around 1e15
        # - ns around 1e18
        out = out.with_columns(
            pl.when(pl.col("ts").is_null())
            .then(pl.lit(None).cast(pl.Int64))
            .when(pl.col("ts") >= 100_000_000_000_000_000)  # >=1e17 -> ns
            .then((pl.col("ts") // 1_000_000).cast(pl.Int64))
            .when(pl.col("ts") >= 100_000_000_000_000)  # >=1e14 -> us
            .then((pl.col("ts") // 1_000).cast(pl.Int64))
            .when(pl.col("ts") < 100_000_000_000)  # <1e11 -> seconds
            .then((pl.col("ts") * 1_000).cast(pl.Int64))
            .otherwise(pl.col("ts").cast(pl.Int64))
            .alias("ts")
        )

        # keep original time_key (for debugging) by not renaming; downstream always uses ts
        return out

    def _read_zip_csv(self, zip_path: str, dataset_type: str) -> pl.DataFrame:
        schema = self.schema_registry.get_schema(dataset_type)
        expected_names = [c["name"] for c in schema["columns"]] if schema and schema.get("columns") else None
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_files:
                return pl.DataFrame()
            content = zf.read(csv_files[0])

        # 參考 SchemaInspector 的邏輯：判斷是否有表頭
        first = content.splitlines()[0].decode("utf-8", errors="ignore").strip() if content else ""
        tokens = [t.strip() for t in first.split(",")] if first else []
        has_header = self._looks_like_header(tokens, expected_names)

        if has_header:
            return pl.read_csv(io.BytesIO(content), infer_schema_length=200)

        # no header
        ncols = len(tokens) if tokens else (len(expected_names) if expected_names else 0)
        if expected_names and len(expected_names) == ncols:
            new_cols = expected_names
        else:
            new_cols = [f"col_{i}" for i in range(ncols)]
        return pl.read_csv(io.BytesIO(content), has_header=False, new_columns=new_cols, infer_schema_length=200)

    def _looks_like_header(self, tokens: List[str], expected_names: Optional[List[str]]) -> bool:
        if not tokens:
            return True
        if expected_names:
            exp = set(expected_names)
            hit = sum(1 for t in tokens if t in exp)
            if hit >= max(1, len(tokens) // 2):
                return True
            return False
        id_like = 0
        val_like = 0
        for t in tokens:
            if self._is_bool(t) or self._is_number(t):
                val_like += 1
            else:
                if any(ch.isalpha() for ch in t) or "_" in t:
                    id_like += 1
                else:
                    val_like += 1
        return id_like > val_like

    def _is_bool(self, t: str) -> bool:
        return t.lower() in ("true", "false")

    def _is_number(self, t: str) -> bool:
        try:
            float(t)
            return True
        except Exception:
            return False

    def _fill_missing(self, df: pl.DataFrame, strategy: str) -> Dict[str, Any]:
        if "ts" in df.columns:
            df = df.sort("ts")

        numeric_cols = []
        for c, dt in zip(df.columns, df.dtypes):
            if c == "ts":
                continue
            if dt in (pl.Int64, pl.Int32, pl.Float64, pl.Float32, pl.UInt64, pl.UInt32):
                numeric_cols.append(c)

        nulls_before = {c: int(df.select(pl.col(c).null_count()).item()) for c in numeric_cols}

        if strategy not in ("adjacent_avg", "ffill"):
            return {"df": df, "strategy": strategy, "nulls_before": nulls_before, "nulls_after": nulls_before}

        if strategy == "ffill":
            exprs = []
            for c in numeric_cols:
                exprs.append(pl.col(c).fill_null(strategy="forward").alias(c))
            df2 = df.with_columns(exprs) if exprs else df
            nulls_after = {c: int(df2.select(pl.col(c).null_count()).item()) for c in numeric_cols}
            filled = {c: max(0, nulls_before[c] - nulls_after[c]) for c in numeric_cols}
            total_filled = int(sum(filled.values()))
            return {
                "df": df2,
                "strategy": "ffill (forward fill)",
                "nulls_before": nulls_before,
                "nulls_after": nulls_after,
                "filled_counts": filled,
                "total_filled": total_filled,
                "note": "適用於 funding rate / OI 這類低頻資料對齊高頻時的『保持最新值』補洞。",
            }

        # 線性插值：內部缺口會變成相鄰兩點的平均（如果 gap=1）；gap>1 會做線性
        exprs = []
        for c in numeric_cols:
            exprs.append(pl.col(c).cast(pl.Float64).interpolate().alias(c))
        df2 = df.with_columns(exprs) if exprs else df

        nulls_after = {c: int(df2.select(pl.col(c).null_count()).item()) for c in numeric_cols}
        filled = {c: max(0, nulls_before[c] - nulls_after[c]) for c in numeric_cols}
        total_filled = int(sum(filled.values()))

        return {
            "df": df2,
            "strategy": "adjacent_avg (linear interpolation)",
            "nulls_before": nulls_before,
            "nulls_after": nulls_after,
            "filled_counts": filled,
            "total_filled": total_filled,
            "note": "邊界（最前/最後）若缺值，插值無法補齊，會保留為 null 並在報告中反映。",
        }


