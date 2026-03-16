"""
Streamlit UI 應用
"""

import streamlit as st
import sys
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.catalog.database import CatalogDB
from src.catalog.coverage import CoverageAnalyzer
from src.catalog.builder import CatalogBuilder
from src.schema.registry import SchemaRegistry
from src.menu.top10 import TOP10_USDT_SYMBOLS
from src.composer.interactive_builder import InteractiveDatasetBuilder, Selection, BuildConfig
from src.cache.raw_cache import RawParquetCache
import plotly.express as px
import pandas as pd
import requests
import re
import calendar
from datetime import datetime as _dt
import json
import polars as pl
from ui.table_paster import render_table_paster


st.set_page_config(page_title="Binance Vision 資料統整系統", layout="wide")

st.title("📊 Binance Vision 資料統整系統")

# 側邊欄
st.sidebar.title("導航")
page = st.sidebar.selectbox(
    "選擇頁面",
    ["Data Menu", "Coverage Matrix", "Schema Dictionary", "Recipe Composer", "File Viewer", "Table Paster"]
)

# 初始化資料庫連接
@st.cache_resource
def get_catalog_db():
    return CatalogDB("catalog.db")

@st.cache_resource
def get_schema_registry():
    return SchemaRegistry("schema.db")

catalog_db = get_catalog_db()
schema_registry = get_schema_registry()
analyzer = CoverageAnalyzer(catalog_db)

if page == "Data Menu":
    st.header("🗂️ Data Menu（Spot + Futures UM，前十大）")

    st.info("這個頁面是你要的『大菜單』：用下拉式選單看每個標的在不同資料分類下的起訖時間與表頭。")

    # market / symbol / dataset selection
    colA, colB, colC, colD = st.columns([1.2, 1.2, 1.4, 1.2])
    with colA:
        market = st.selectbox("Market", ["futures_um", "spot"])
    with colB:
        symbol = st.selectbox("Symbol", TOP10_USDT_SYMBOLS)

    # 讀取 coverage 資料（若尚未建立 menu-build，會是空）
    with catalog_db.get_session() as session:
        from src.catalog.database import Coverage
        cov_rows = session.query(Coverage).filter_by(market=market, symbol=symbol).all()

    available_datasets = sorted({r.dataset_type for r in cov_rows})
    default_datasets = {
        "spot": ["aggTrades", "klines", "trades"],
        "futures_um": ["aggTrades", "bookDepth", "bookTicker", "fundingRate", "indexPriceKlines", "klines", "markPriceKlines", "metrics", "premiumIndexKlines", "trades"],
    }

    dataset_list = default_datasets.get(market, [])
    # 若 DB 已有更多 dataset，就補進來
    for ds in available_datasets:
        if ds not in dataset_list:
            dataset_list.append(ds)

    with colC:
        dataset_type = st.selectbox("Dataset Type", dataset_list)

    # cadence / interval
    with colD:
        cadence = st.selectbox("Cadence", ["daily", "monthly"])

    interval = None
    interval_datasets = {"klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"}
    if dataset_type in interval_datasets:
        # 從 coverage 中找 interval 選項
        intervals = sorted({r.interval for r in cov_rows if r.dataset_type == dataset_type and r.cadence == cadence and r.interval})
        if not intervals:
            intervals = ["1m", "5m", "1h", "1d"]
        interval = st.selectbox("Interval", intervals)

    # 查 coverage row
    selected_row = None
    for r in cov_rows:
        if r.dataset_type != dataset_type:
            continue
        if r.cadence != cadence:
            continue
        if (dataset_type in interval_datasets) and (r.interval != interval):
            continue
        if (dataset_type not in interval_datasets) and (r.interval is not None):
            continue
        selected_row = r
        break

    if not cov_rows:
        st.warning("你還沒建立『大菜單』coverage。請先在 PowerShell 跑：`python -m cli.main menu-build --include-monthly`")
    elif not selected_row or not selected_row.start_date or not selected_row.end_date:
        st.warning("這個組合目前查不到資料（可能該 market 不提供此 dataset，或該 symbol 沒有）。")
    else:
        st.subheader(f"{symbol} / {market} / {dataset_type} / {cadence}" + (f" / {interval}" if interval else ""))

        # 顯示起訖時間
        st.write(f"**可用範圍**：`{selected_row.start_date}` ~ `{selected_row.end_date}`")

        # 顯示 sample 檔資訊（現場 HEAD，不掃全量）
        builder = CatalogBuilder(catalog_db, market=market)
        http = requests.Session()
        http.headers.update({"User-Agent": "Binance-Vision-MenuUI/1.0"})

        def head_url(d_str: str):
            url = builder._build_url(symbol, dataset_type, d_str, cadence, interval)
            r = http.head(url, timeout=20, allow_redirects=True)
            return {
                "url": url,
                "status": r.status_code,
                "size": r.headers.get("Content-Length"),
                "last_modified": r.headers.get("Last-Modified"),
                "etag": r.headers.get("ETag"),
            }

        meta_start = head_url(selected_row.start_date)
        meta_end = head_url(selected_row.end_date)

        meta_df = pd.DataFrame([
            {"Item": "start_file", **meta_start},
            {"Item": "end_file", **meta_end},
        ])
        st.write("**樣本檔案（HEAD）**")
        st.dataframe(meta_df, use_container_width=True)

        # 顯示 schema（若已建立）
        st.write("**表頭 / 欄位（Schema）**")
        schema = schema_registry.get_schema(dataset_type)
        if not schema:
            st.info("schema.db 尚未有此 dataset 的表頭。下一步我會加『從 catalog 自動抽樣建 schema』的一鍵指令。")
        else:
            cols = []
            for col in schema["columns"]:
                cols.append({
                    "name": col["name"],
                    "dtype": schema["dtypes"].get(col["name"], "unknown"),
                    "note": schema.get("field_notes", {}).get(col["name"], ""),
                })
            st.dataframe(pd.DataFrame(cols), use_container_width=True)

        # 顯示你想要的「目錄感」：dataset 清單
        st.write("**目錄（模擬）**")
        menu_rows = [{"Item": "../", "Size": "", "Last Modified": ""}]
        for ds in default_datasets.get(market, []):
            menu_rows.append({"Item": f"{ds}/", "Size": "", "Last Modified": ""})
        st.dataframe(pd.DataFrame(menu_rows), use_container_width=True)

elif page == "Coverage Matrix":
    st.header("📈 Coverage Matrix")
    
    # 設定
    col1, col2 = st.columns(2)
    with col1:
        symbols_input = st.text_input("交易對（逗號分隔）", value="BTCUSDT,ETHUSDT,BNBUSDT")
        symbols = [s.strip() for s in symbols_input.split(",")]
    
    with col2:
        dataset_types_input = st.text_input("資料集類型（留空=全部）", value="")
        dataset_types = [ds.strip() for ds in dataset_types_input.split(",")] if dataset_types_input else None
    
    if st.button("查詢 Coverage"):
        matrix = analyzer.get_coverage_matrix(symbols, dataset_types)
        
        # 建立表格
        rows = []
        for symbol, datasets in matrix.items():
            for ds_type, info in datasets.items():
                rows.append({
                    "Symbol": symbol,
                    "Dataset": ds_type,
                    "Start Date": info["start_date"],
                    "End Date": info["end_date"],
                    "Files": info["num_files"],
                    "Missing": info["num_missing"],
                    "Size (MB)": round(info["total_size"] / 1024 / 1024, 2) if info["total_size"] else 0
                })
        
        if rows:
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True)
            
            # 視覺化
            if len(rows) > 0:
                fig = px.scatter(
                    df,
                    x="Start Date",
                    y="Dataset",
                    size="Files",
                    color="Symbol",
                    hover_data=["Missing", "Size (MB)"],
                    title="Coverage Overview"
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("沒有找到資料，請先執行 catalog build")

elif page == "Schema Dictionary":
    st.header("📚 Schema Dictionary")
    
    dataset_type = st.selectbox(
        "選擇資料集類型",
        ["klines", "aggTrades", "trades", "bookTicker", "bookDepth", 
         "indexPriceKlines", "markPriceKlines", "premiumIndexKlines", "fundingRate", "metrics"]
    )
    
    schema = schema_registry.get_schema(dataset_type)
    
    if schema:
        st.subheader(f"Schema: {dataset_type}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write("**基本資訊**")
            st.write(f"- Version: {schema['version']}")
            st.write(f"- Primary Time Key: {schema['primary_time_key']}")
            st.write(f"- Join Key: {schema['join_key']}")
            st.write(f"- Validation: {schema['validation_status']}")
        
        with col2:
            st.write("**欄位列表**")
            for col in schema["columns"]:
                dtype = schema["dtypes"].get(col["name"], "unknown")
                note = schema["field_notes"].get(col["name"], "")
                note_text = f" ({note})" if note else ""
                st.write(f"- `{col['name']}`: {dtype}{note_text}")
        
        # 顯示樣本
        samples = schema_registry.get_samples(dataset_type)
        if samples:
            st.subheader("樣本資料")
            with st.expander("查看樣本"):
                for i, sample in enumerate(samples[:3]):
                    st.write(f"**樣本 {i+1}** ({sample['symbol']}, {sample['sample_date']})")
                    if sample["first_n_rows"]:
                        sample_df = pd.DataFrame(sample["first_n_rows"][:5])
                        st.dataframe(sample_df)
    else:
        st.warning(f"Schema 尚未註冊，請先執行 schema inspection")

elif page == "Recipe Composer":
    st.header("Dataset Builder（自組裝車）")

    st.write("你可以建立多個『清單/組合』，每個清單加入多個來源（選 market/symbol/dataset/interval，欄位可選全部或勾選），再按一下就產出 Parquet + 報告。")

    if "recipes" not in st.session_state:
        st.session_state["recipes"] = []

    def new_recipe():
        return {
            "name": f"dataset_{len(st.session_state['recipes'])+1}",
            "start": "2022-01-01",
            "end": "2024-12-31",
            "anchor_index": 0,
            "selections": [],
        }

    colA, colB = st.columns([1, 3])
    with colA:
        if st.button("新增清單"):
            st.session_state["recipes"].append(new_recipe())
    with colB:
        if not st.session_state["recipes"]:
            st.info("尚未有清單，請先按『新增清單』。")
            st.stop()

    recipe_names = [r["name"] for r in st.session_state["recipes"]]
    recipe_idx = st.selectbox("選擇清單", list(range(len(recipe_names))), format_func=lambda i: recipe_names[i])
    recipe = st.session_state["recipes"][recipe_idx]

    # 基本設定
    col1, col2, col3 = st.columns(3)
    with col1:
        recipe["name"] = st.text_input("清單名稱", value=recipe["name"])
    with col2:
        recipe["start"] = st.text_input("開始日期 (YYYY-MM-DD)", value=recipe["start"])
    with col3:
        recipe["end"] = st.text_input("結束日期 (YYYY-MM-DD)", value=recipe["end"])

    st.subheader("新增來源（market / symbol / dataset / interval / 欄位）")
    # 來源選擇
    colx1, colx2, colx3, colx4 = st.columns([1.2, 1.2, 1.6, 1.0])
    with colx1:
        sel_market = st.selectbox("Market", ["futures_um", "spot"], key=f"m_{recipe_idx}")
    with colx2:
        sel_symbol = st.selectbox("Symbol", TOP10_USDT_SYMBOLS, key=f"s_{recipe_idx}")
    with colx3:
        if sel_market == "spot":
            ds_list = ["klines", "trades", "aggTrades"]
        else:
            ds_list = ["aggTrades", "bookDepth", "bookTicker", "fundingRate", "indexPriceKlines", "klines", "markPriceKlines", "metrics", "premiumIndexKlines", "trades"]
        sel_dataset = st.selectbox("Dataset", ds_list, key=f"d_{recipe_idx}")
    with colx4:
        sel_cadence = st.selectbox("Cadence", ["daily"], key=f"c_{recipe_idx}")

    sel_interval = None
    if sel_dataset in {"klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"}:
        sel_interval = st.selectbox("Interval", ["1m", "5m", "15m", "1h", "4h", "1d"], key=f"i_{recipe_idx}")

    schema = schema_registry.get_schema(sel_dataset)
    schema_cols = [c["name"] for c in schema["columns"]] if schema and schema.get("columns") else []
    schema_cols_no_ignore = [c for c in schema_cols if c.lower() != "ignore"]

    coly1, coly2 = st.columns([1.2, 3.0])
    with coly1:
        use_all = st.checkbox("全部欄位（排除 ignore）", value=True, key=f"all_{recipe_idx}")
    with coly2:
        chosen_cols = []
        if not use_all:
            chosen_cols = st.multiselect("選擇欄位", schema_cols_no_ignore, default=[], key=f"cols_{recipe_idx}")
        else:
            st.write(f"將加入 {len(schema_cols_no_ignore)} 個欄位")

    if st.button("加入到清單", key=f"add_{recipe_idx}"):
        recipe["selections"].append({
            "market": sel_market,
            "symbol": sel_symbol,
            "dataset_type": sel_dataset,
            "cadence": sel_cadence,
            "interval": sel_interval,
            "use_all_columns": use_all,
            "columns": chosen_cols,
        })
        if recipe["anchor_index"] >= len(recipe["selections"]):
            recipe["anchor_index"] = 0

    st.subheader("清單內容（來源列表）")
    if not recipe["selections"]:
        st.info("尚未加入任何來源。")
    else:
        sel_rows = []
        for i, s in enumerate(recipe["selections"]):
            sel_rows.append({
                "#": i,
                "market": s["market"],
                "symbol": s["symbol"],
                "dataset": s["dataset_type"],
                "cadence": s.get("cadence", "daily"),
                "interval": s.get("interval") or "",
                "columns": "ALL(no ignore)" if s.get("use_all_columns") else ",".join(s.get("columns") or []),
            })
        st.dataframe(pd.DataFrame(sel_rows), use_container_width=True)

        anchor_labels = [
            f"{i}: {s['symbol']} / {s['market']} / {s['dataset_type']}" + (f" / {s['interval']}" if s.get("interval") else "")
            for i, s in enumerate(recipe["selections"])
        ]
        recipe["anchor_index"] = st.selectbox("主時間軸（以此來源的時間對齊）", list(range(len(anchor_labels))), index=recipe["anchor_index"], format_func=lambda i: anchor_labels[i])

        colz1, colz2 = st.columns([1, 2])
        with colz1:
            if st.button("移除最後一個來源"):
                recipe["selections"].pop()
                recipe["anchor_index"] = min(recipe["anchor_index"], max(0, len(recipe["selections"]) - 1))
                st.rerun()
        with colz2:
            st.caption("缺值策略：以相鄰兩值平均（線性插值）。缺哪些日期與補了多少會寫進 report.json。")

    st.subheader("產出 / 下載")
    st.write("輸出：Parquet（建議給 ML）+ report.json（缺口與補值回報）")
    out_dir = st.text_input("輸出資料夾", value="data/outputs/datasets", key=f"out_{recipe_idx}")

    fill_strategy = st.selectbox(
        "缺值策略（不同時間尺度資料對齊後）",
        ["ffill", "adjacent_avg"],
        index=0,
        help="ffill：低頻資料（funding/OI）對齊到高頻時，用『保持最新值』補洞；adjacent_avg：線性插值（相鄰平均）。",
        key=f"fill_{recipe_idx}",
    )

    st.subheader("預先快取（raw -> parquet 分區）")
    st.write("建議先做一次快取：之後組裝不同清單時會直接讀 parquet，不用再重複下載/解壓。")
    cache_threads = st.number_input("快取執行緒數", min_value=1, max_value=64, value=32, step=1, key=f"thr_{recipe_idx}")
    prefer_monthly = st.checkbox("優先使用 monthly（缺的再用 daily 補洞）", value=True, key=f"pm_{recipe_idx}")
    force_rebuild = st.checkbox("強制重建快取（覆寫既有 raw parquet，修復 dtype 不一致等問題）", value=False, key=f"force_{recipe_idx}")

    if st.button("開始預先快取（此清單所有來源）", key=f"precache_{recipe_idx}"):
        if not recipe["selections"]:
            st.error("清單沒有來源，請先加入來源。")
            st.stop()
        cache = RawParquetCache(cache_root="data/raw_parquet", zip_root="data/raw_zips", schema_db_path="schema.db", threads=int(cache_threads))
        all_manifests = []
        with st.spinner("正在建立 raw parquet 快取..."):
            for s in recipe["selections"]:
                sel = Selection(**s)
                tasks, _ = cache.plan_tasks(
                    market=sel.market,
                    dataset_type=sel.dataset_type,
                    symbol=sel.symbol,
                    start=recipe["start"],
                    end=recipe["end"],
                    interval=sel.interval,
                    prefer_monthly=prefer_monthly,
                )
                manifest = cache.build_cache(
                    tasks,
                    prefer_monthly=prefer_monthly,
                    manifest_name=f"ui_cache_{sel.market}_{sel.symbol}_{sel.dataset_type}",
                    force_rebuild=force_rebuild,
                )
                all_manifests.append(manifest)
        st.success("快取完成（或已存在而跳過）")
        st.write("manifest（摘要）")
        st.dataframe(pd.DataFrame([m["summary"] for m in all_manifests]), use_container_width=True)

    if st.button("開始產出（下載 + 合併）", key=f"run_{recipe_idx}"):
        if not recipe["selections"]:
            st.error("清單沒有來源，請先加入來源。")
            st.stop()

        def normalize_date_range(start_s: str, end_s: str) -> tuple[str, str]:
            """
            允許輸入：
            - YYYY
            - YYYY-MM
            - YYYY-MM-DD
            會轉成 YYYY-MM-DD（start=月初/年初，end=月底/年末）。
            """
            start_s = (start_s or "").strip()
            end_s = (end_s or "").strip()

            def parse_one(s: str, is_end: bool) -> str:
                if re.fullmatch(r"\d{4}", s):
                    y = int(s)
                    return f"{y:04d}-12-31" if is_end else f"{y:04d}-01-01"
                if re.fullmatch(r"\d{4}-\d{2}", s):
                    y, m = s.split("-")
                    y = int(y); m = int(m)
                    last = calendar.monthrange(y, m)[1]
                    return f"{y:04d}-{m:02d}-{last:02d}" if is_end else f"{y:04d}-{m:02d}-01"
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
                    # validate
                    _dt.strptime(s, "%Y-%m-%d")
                    return s
                raise ValueError(f"日期格式錯誤：{s}（允許 YYYY / YYYY-MM / YYYY-MM-DD）")

            s0 = parse_one(start_s, is_end=False)
            s1 = parse_one(end_s, is_end=True)
            if _dt.strptime(s0, "%Y-%m-%d") > _dt.strptime(s1, "%Y-%m-%d"):
                raise ValueError(f"開始日期 {s0} 不能晚於結束日期 {s1}")
            return s0, s1

        try:
            norm_start, norm_end = normalize_date_range(recipe["start"], recipe["end"])
        except Exception as e:
            st.error(str(e))
            st.stop()

        builder = InteractiveDatasetBuilder("catalog.db", "schema.db")
        sels = []
        for s in recipe["selections"]:
            sels.append(Selection(**s))
        cfg = BuildConfig(
            name=recipe["name"],
            start=norm_start,
            end=norm_end,
            anchor_index=int(recipe["anchor_index"]),
            output_dir=out_dir,
            download_dir="data/downloads",
            fill_strategy=fill_strategy,
        )
        with st.spinner("正在下載與合併...（第一次會比較久）"):
            parquet_path, report_path = builder.build(sels, cfg)
        st.success("完成")
        st.write(f"- Parquet：`{parquet_path}`")
        st.write(f"- 報告：`{report_path}`")

elif page == "File Viewer":
    st.header("🗃️ File Viewer（檢視已存好的檔案）")
    st.info("用這頁檢視你已產出的 dataset（parquet / report / recipe）或 raw cache 的 manifest。")

    tab_ds, tab_cache = st.tabs(["Outputs Datasets", "Raw Cache Manifests"])

    with tab_ds:
        root = st.text_input("datasets 根目錄", value="data/outputs/datasets")
        root_p = Path(root)
        if not root_p.exists():
            st.error(f"找不到：{root_p}")
        else:
            ds_dirs = sorted([p for p in root_p.iterdir() if p.is_dir()], key=lambda p: p.name.lower())
            if not ds_dirs:
                st.warning("目前沒有任何輸出資料夾（data/outputs/datasets/*）。")
            else:
                ds_name = st.selectbox("選擇 dataset", [p.name for p in ds_dirs])
                ds_dir = root_p / ds_name

                parquet_files = sorted(ds_dir.glob("*.parquet"))
                json_files = sorted(ds_dir.glob("*.json"))

                col1, col2 = st.columns([1, 1])
                with col1:
                    st.subheader("report / recipe")
                    for jf in json_files:
                        with st.expander(jf.name, expanded=("report" in jf.name)):
                            try:
                                obj = json.loads(jf.read_text(encoding="utf-8"))
                                st.json(obj)
                            except Exception as e:
                                st.error(f"讀取失敗：{e}")

                with col2:
                    st.subheader("Parquet")
                    if not parquet_files:
                        st.warning("此資料夾沒有 parquet。")
                    else:
                        pq = st.selectbox("選擇 parquet", [p.name for p in parquet_files])
                        pq_path = ds_dir / pq
                        st.write(f"檔案：`{pq_path}`")

                        try:
                            lf = pl.scan_parquet(str(pq_path))
                            sch = lf.collect_schema()
                            schema_names = sch.names()
                            schema_dtypes = sch.dtypes()
                            schema_df = pd.DataFrame({"column": schema_names, "dtype": [str(x) for x in schema_dtypes]})
                            st.dataframe(schema_df, use_container_width=True, height=260)

                            show_stats = st.checkbox("計算筆數/ts 範圍（可能較慢）", value=False)
                            if show_stats:
                                nrows = lf.select(pl.len()).collect().item()
                                st.write(f"rows: **{nrows:,}**")
                                if "ts" in schema_names:
                                    ts_minmax = lf.select([pl.col("ts").min().alias("min_ts"), pl.col("ts").max().alias("max_ts")]).collect().to_dicts()[0]
                                    st.write(f"ts range: `{ts_minmax['min_ts']}` ~ `{ts_minmax['max_ts']}`")

                            preview_n = st.slider("預覽筆數", min_value=10, max_value=500, value=50, step=10)
                            preview_mode = st.radio("預覽模式", ["head", "tail"], horizontal=True)
                            cols_pick = st.multiselect("只顯示部分欄位（不選＝全部）", schema_names, default=[])

                            lf2 = lf
                            if cols_pick:
                                if "ts" in schema_names:
                                    lf2 = lf.select(["ts"] + [c for c in cols_pick if c != "ts"])
                                else:
                                    lf2 = lf.select(cols_pick)

                            df_prev = lf2.head(preview_n).collect() if preview_mode == "head" else lf2.tail(preview_n).collect()
                            st.dataframe(df_prev.to_pandas(), use_container_width=True, height=420)

                            null_limit_default = 200
                            null_limit_max = max(10, len(schema_names))
                            null_limit = st.number_input(
                                "缺值統計欄位上限（0=全部；欄位很多時建議先用 200~1000）",
                                min_value=0,
                                max_value=null_limit_max,
                                value=min(null_limit_default, null_limit_max),
                                step=50,
                                key=f"null_limit_{ds_name}_{pq}",
                            )

                            if st.button("計算缺值統計（null_count）", key=f"null_{ds_name}_{pq}"):
                                cols_for_null = cols_pick or schema_names
                                if int(null_limit) > 0:
                                    cols_for_null = cols_for_null[: int(null_limit)]
                                null_df = lf.select([pl.col(c).null_count().alias(c) for c in cols_for_null]).collect()
                                st.dataframe(null_df.to_pandas().T.rename(columns={0: "null_count"}), use_container_width=True, height=420)

                        except Exception as e:
                            st.error(f"Parquet 讀取失敗：{e}")

    with tab_cache:
        cache_root = st.text_input("raw cache manifest 根目錄", value="data/raw_parquet")
        cache_p = Path(cache_root)
        if not cache_p.exists():
            st.error(f"找不到：{cache_p}")
        else:
            manifests = sorted([p for p in cache_p.glob("*.json") if p.is_file()], key=lambda p: p.stat().st_mtime, reverse=True)
            if not manifests:
                st.warning("目前沒有任何 manifest（data/raw_parquet/*.json）。")
            else:
                mf = st.selectbox("選擇 manifest", [p.name for p in manifests])
                mf_path = cache_p / mf
                st.write(f"檔案：`{mf_path}`")
                try:
                    obj = json.loads(mf_path.read_text(encoding="utf-8"))
                    st.json(obj)
                except Exception as e:
                    st.error(f"讀取失敗：{e}")

elif page == "Table Paster":
    render_table_paster()

# 頁尾
st.sidebar.markdown("---")
st.sidebar.markdown("**Binance Vision 資料統整系統 v0.1.0**")

