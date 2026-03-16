from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
import polars as pl
import streamlit as st
import json

from src.features.formula_engine import ColumnSpec, FormulaEngine


def _list_dataset_dirs(root: Path) -> List[Path]:
    if not root.exists():
        return []
    return sorted([p for p in root.iterdir() if p.is_dir()], key=lambda p: p.name.lower())


def _ts_to_right(cols: List[str]) -> List[str]:
    if "ts" not in cols:
        return cols
    return [c for c in cols if c != "ts"] + ["ts"]


def _unique_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _format_polars_error(e: Exception, max_len: int = 500) -> str:
    """
    Polars 在錯誤訊息中常會附帶整段 query plan（WITH_COLUMNS / Parquet SCAN ...），
    UI 直接印出來會非常吵且難以定位真正原因。
    這裡把訊息裁切成「主要錯誤原因」；完整內容可另行顯示。
    """
    msg = str(e) or ""
    # 常見：在 collect/sink 失敗時會附上 query plan
    cut_markers = [
        "\n\nResolved plan until failure:",
        "\nResolved plan until failure:",
        "\n\nLogical plan:",
        "\nLogical plan:",
        "\n\nPhysical plan:",
        "\nPhysical plan:",
    ]
    for m in cut_markers:
        if m in msg:
            msg = msg.split(m, 1)[0].rstrip()
            break
    msg = msg.replace("\r\n", "\n").strip()
    head = msg.splitlines()[0].strip() if msg else ""
    pretty = head or msg or repr(e)
    if len(pretty) > max_len:
        pretty = pretty[: max_len - 3] + "..."
    return f"{type(e).__name__}: {pretty}" if not pretty.startswith(type(e).__name__) else pretty


def render_table_paster():
    st.header("表格拼貼器（公式加欄 / 橫向擴充）")
    st.info(
        "流程：先選一個『已對齊好的 parquet 寬表』→ 下面用公式新增特徵欄位 → 先看範例 rows → 確認後輸出成新 parquet。"
    )
    with st.expander("公式表（可用函數/語法/範例）", expanded=False):
        p = Path("FORMULA_TABLE.md")
        if p.exists():
            st.markdown(p.read_text(encoding="utf-8"))
        else:
            st.warning("找不到 FORMULA_TABLE.md（你可以在專案根目錄新增/編輯）。")

    root = Path(st.text_input("datasets 根目錄", value="data/outputs/datasets"))
    ds_dirs = _list_dataset_dirs(root)
    if not ds_dirs:
        st.warning("找不到任何輸出資料夾（data/outputs/datasets/*）。先去 Recipe Composer 產出一份 parquet 吧。")
        return

    ds_name = st.selectbox("選擇 dataset", [p.name for p in ds_dirs], index=0)
    ds_dir = root / ds_name

    parquet_files = sorted(ds_dir.glob("*.parquet"))
    if not parquet_files:
        st.warning("這個 dataset 資料夾沒有 parquet。")
        return

    pq_name = st.selectbox("選擇 parquet", [p.name for p in parquet_files], index=0)
    pq_path = ds_dir / pq_name
    st.caption(f"檔案：`{pq_path}`")

    # 讀 schema（不讀全量）
    lf0 = pl.scan_parquet(str(pq_path))
    schema = lf0.collect_schema()
    cols = schema.names()

    if "ts" not in cols:
        st.error("這份 parquet 沒有 `ts` 欄位，無法當作對齊主軸使用。")
        return

    # Preview 原始表
    st.subheader("原始表預覽")
    c1, c2 = st.columns([1, 1])
    with c1:
        preview_n = st.slider("預覽筆數（head）", 10, 2000, 200, 10)
    with c2:
        preview_cols_limit = st.slider("預覽欄位上限（避免太寬畫面空白）", 10, 200, 60, 10)

    show_cols = _ts_to_right(cols)[:preview_cols_limit]
    base_preview = lf0.sort("ts").select(show_cols).head(preview_n).collect()
    st.dataframe(base_preview.to_pandas(), use_container_width=True, height=360)

    st.subheader("新增特徵（公式）")
    st.caption(
        "小提示：欄位名可直接用變數（例如 BTCUSDT_klines_1m_close），或用 COL(\"欄位名\")。"
        " 想『不計算、只搬運原始欄位』：formula 直接填欄位名即可（例如 BTCUSDT_klines_1h_volume）。"
        " 若輸出模式選『只輸出新欄位 + ts』，請把 name 取成不同名字（例如 BTC_VOL），否則會被視為原始欄位而不會出現在『只看新欄位』清單。"
        " 橫向運算可用 COLS(\"regex\") + ROW_MEAN/ROW_SUM。"
    )

    # Session state for specs
    key = f"tp_specs::{ds_name}::{pq_name}"
    if key not in st.session_state:
        st.session_state[key] = [
            {"name": "btc_ret_1", "formula": "LOGRET(COL('BTCUSDT_klines_1m_close'), 1)"},
            {"name": "eth_ret_1", "formula": "LOGRET(COL('ETHUSDT_klines_1m_close'), 1)"},
        ]

    specs_df = pd.DataFrame(st.session_state[key])
    edited = st.data_editor(
        specs_df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "name": st.column_config.TextColumn("欄位名稱", required=True),
            "formula": st.column_config.TextColumn("公式", required=True),
        },
        key=f"tp_editor::{ds_name}::{pq_name}",
    )
    st.session_state[key] = edited.to_dict(orient="records")

    # 明確的刪除 UI（避免 data_editor 不好刪）
    cur_names = []
    for r in st.session_state.get(key, []):
        n = (r.get("name") or "").strip()
        if n:
            cur_names.append(n)
    cur_names = sorted(set(cur_names))

    del_cols = st.multiselect("刪除欄位（多選）", cur_names, default=[], key=f"tp_del_pick::{ds_name}::{pq_name}")
    if st.button("刪除選取欄位", key=f"tp_del_btn::{ds_name}::{pq_name}"):
        if not del_cols:
            st.warning("請先選要刪除的欄位。")
        else:
            st.session_state[key] = [r for r in st.session_state[key] if (r.get("name") or "").strip() not in set(del_cols)]
            st.rerun()

    # Global apply (simplified per request)
    with st.expander("全欄位公式（批次覆蓋：把模板包在既有公式外層）", expanded=False):
        st.caption(
            "只保留兩件事：\n"
            "- 覆蓋原欄位（直接改公式）\n"
            "- 把模板套在『既有公式』外層（例如 ZSCORE({x}) 會變成 ZSCORE(<原本公式>))"
        )

        # collect existing specs (name -> formula)
        specs_map = {}
        for r in st.session_state.get(key, []):
            n = (r.get("name") or "").strip()
            f = (r.get("formula") or "").strip()
            if n:
                specs_map[n] = f

        all_names = sorted(specs_map.keys())
        pick_key = f"tp_wrap_pick::{ds_name}::{pq_name}"
        if pick_key not in st.session_state:
            st.session_state[pick_key] = []

        colA, colB, colC = st.columns([1, 1, 2])
        with colA:
            if st.button("全選", key=f"tp_wrap_pick_all::{ds_name}::{pq_name}"):
                st.session_state[pick_key] = list(all_names)
                st.rerun()
        with colB:
            if st.button("清除", key=f"tp_wrap_pick_clear::{ds_name}::{pq_name}"):
                st.session_state[pick_key] = []
                st.rerun()
        with colC:
            st.caption("先選要覆蓋的欄位，再按「套用模板」")

        pick = st.multiselect("選擇要套用的欄位（可多選）", all_names, default=st.session_state[pick_key], key=pick_key)
        template = st.text_input("模板（必須包含 {x}）", value="ZSCORE({x})", key=f"tp_wrap_tpl::{ds_name}::{pq_name}")

        if st.button("套用模板（覆蓋原欄位）", type="primary", key=f"tp_wrap_apply::{ds_name}::{pq_name}"):
            if "{x}" not in (template or ""):
                st.error("模板必須包含 `{x}`。")
            elif not pick:
                st.warning("請先選擇要套用的欄位（可按上方『全選』）。")
            else:
                overwrite = {}
                for name in pick:
                    base_formula = (specs_map.get(name) or "").strip()
                    if not base_formula:
                        continue
                    overwrite[name] = template.replace("{x}", f"({base_formula})")

                merged = []
                for r in st.session_state[key]:
                    n = (r.get("name") or "").strip()
                    if n in overwrite:
                        merged.append({"name": n, "formula": overwrite[n]})
                    else:
                        merged.append(r)
                st.session_state[key] = merged
                st.success(f"已覆蓋 {len(overwrite)} 個欄位公式")
                st.rerun()

    def _compute_lf2():
        engine = FormulaEngine(cols)
        specs = [
            ColumnSpec(**r)
            for r in st.session_state[key]
            if (r.get("name") or "").strip() or (r.get("formula") or "").strip()
        ]
        # 檢查重複欄位名：Polars 在 select/projection 會直接 DuplicateError，先在 UI 擋下來
        spec_names_raw = [(s.name or "").strip() for s in specs if (s.name or "").strip()]
        dupes = sorted({n for n in spec_names_raw if spec_names_raw.count(n) > 1})
        if dupes:
            errors = [
                {
                    "name": n,
                    "formula": "<duplicate name>",
                    "error": f"欄位名稱重複（請保留一個或改名）：{n}",
                }
                for n in dupes
            ]
            return lf0.sort("ts"), errors, _unique_keep_order(spec_names_raw)

        lf = lf0.sort("ts")
        lf2, errors = engine.apply_specs(lf, specs)
        spec_names = _unique_keep_order([s.name for s in specs if (s.name or "").strip()])
        return lf2, errors, spec_names

    # Apply / Preview
    st.subheader("計算結果（範例）")
    show_only_new = st.checkbox("預設只看新欄位 + ts", value=True, key=f"tp_only_new::{ds_name}::{pq_name}")
    run = st.button("執行預覽計算", type="primary", key=f"tp_run::{ds_name}::{pq_name}")
    if run:
        lf2, errors, spec_names = _compute_lf2()

        if errors:
            st.error("有些公式無法編譯/計算（先修正再輸出）：")
            st.dataframe(pd.DataFrame(errors), use_container_width=True, height=240)

        lf2_cols = lf2.collect_schema().names()
        new_cols_present = _unique_keep_order([n for n in spec_names if n in lf2_cols and n not in cols])
        if not new_cols_present and errors:
            st.info("你選的某些欄位沒有產生出來（通常是公式空白或公式錯誤），所以預覽只會顯示原始欄位。")

        if show_only_new:
            display_cols = _ts_to_right(_unique_keep_order(new_cols_present + ["ts"]))
        else:
            base_cols = [c for c in cols if c != "ts" and c in lf2_cols]
            display_cols = _ts_to_right(_unique_keep_order((new_cols_present + base_cols)[:preview_cols_limit] + ["ts"]))
        display_cols = [c for c in display_cols if c in lf2_cols]
        if "ts" in lf2_cols and "ts" not in display_cols:
            display_cols.append("ts")
        display_cols = _ts_to_right(_unique_keep_order(display_cols))
        try:
            prev = lf2.select(display_cols).head(preview_n).collect()
            st.dataframe(prev.to_pandas(), use_container_width=True, height=520)
        except Exception as e:
            st.error(f"預覽失敗：{_format_polars_error(e)}")
            with st.expander("顯示完整錯誤訊息（含 Polars query plan）", expanded=False):
                st.code(str(e))

    # Export (always available)
    st.subheader("輸出（完整檔案）")

    # 用 form 避免切換 radio 就 rerun 造成頁面捲動「跳轉」
    export_form = st.form(key=f"tp_export_form::{ds_name}::{pq_name}", clear_on_submit=False)
    out_name = export_form.text_input("輸出資料夾名稱", value=f"{ds_name}_features", key=f"tp_out_name::{ds_name}::{pq_name}")
    export_mode = export_form.radio(
        "輸出內容",
        ["全欄位（原始 + 新欄位 + ts）", "只輸出新欄位 + ts"],
        index=0,
        horizontal=True,
        key=f"tp_export_mode::{ds_name}::{pq_name}",
    )
    submitted = export_form.form_submit_button("輸出完整 Parquet（按下才會開始計算）")

    out_root = Path("data/outputs/datasets") / out_name
    out_path = out_root / f"{out_name}.parquet"
    st.caption(f"輸出路徑：`{out_path}`")

    if submitted:
        with st.spinner("正在計算並輸出 parquet..."):
            out_root.mkdir(parents=True, exist_ok=True)
            lf2, errors, spec_names = _compute_lf2()
            if errors:
                st.error("輸出前請先修正所有公式錯誤：")
                st.dataframe(pd.DataFrame(errors), use_container_width=True, height=260)
            else:
                lf2_cols = lf2.collect_schema().names()
                new_cols_present = _unique_keep_order([n for n in spec_names if n in lf2_cols and n not in cols])
                if export_mode.startswith("只輸出新欄位"):
                    final_cols = _ts_to_right(
                        _unique_keep_order([c for c in new_cols_present if c in lf2_cols] + (["ts"] if "ts" in lf2_cols else []))
                    )
                else:
                    final_cols = _ts_to_right(_unique_keep_order(lf2_cols))

                # 存公式表（可重現）
                try:
                    tsv_path = out_root / "formulas.tsv"
                    lines = ["name\tformula"]
                    for r in st.session_state.get(key, []):
                        n = (r.get("name") or "").strip()
                        f = (r.get("formula") or "").strip()
                        if n or f:
                            lines.append(f"{n}\t{f}")
                    tsv_path.write_text("\n".join(lines), encoding="utf-8")

                    recipe_path = out_root / "recipe.json"
                    recipe = {
                        "generated_at": datetime.utcnow().isoformat(),
                        "input_parquet": str(pq_path),
                        "output_parquet": str(out_path),
                        "export_mode": export_mode,
                        "specs": [r for r in st.session_state.get(key, [])],
                    }
                    recipe_path.write_text(json.dumps(recipe, ensure_ascii=False, indent=2), encoding="utf-8")
                except Exception as e:
                    st.warning(f"已輸出 parquet，但附帶寫入 formulas/recipe 失敗：{e}")

                try:
                    lf2.select(final_cols).sink_parquet(str(out_path))
                    # report.json（輸出摘要）
                    try:
                        row_count = lf2.select(pl.len().alias("n")).collect().item()
                    except Exception:
                        row_count = None
                    try:
                        report_path = out_root / "report.json"
                        report = {
                            "generated_at": datetime.utcnow().isoformat(),
                            "input_parquet": str(pq_path),
                            "output_parquet": str(out_path),
                            "export_mode": export_mode,
                            "row_count": row_count,
                            "output_columns": final_cols,
                            "spec_count": len([r for r in st.session_state.get(key, []) if (r.get("name") or "").strip()]),
                            "note": "Table Paster export: parquet + formulas.tsv + recipe.json + report.json",
                        }
                        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
                    except Exception as e:
                        st.warning(f"parquet 已輸出，但 report.json 寫入失敗：{e}")

                    st.success(f"已輸出：`{out_path}`（並寫入 `formulas.tsv` / `recipe.json` / `report.json`）")
                except Exception as e:
                    st.error(f"輸出失敗：{_format_polars_error(e)}")
                    with st.expander("顯示完整錯誤訊息（含 Polars query plan）", expanded=False):
                        st.code(str(e))


