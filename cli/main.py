"""
CLI 主程式
"""

import argparse
import sys
import shutil
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.catalog.database import init_database, CatalogDB
from src.catalog.builder import CatalogBuilder
from src.catalog.coverage import CoverageAnalyzer
from src.catalog.crawler import CoverageCrawler
from src.menu.top10 import TOP10_USDT_SYMBOLS
from src.schema.registry import SchemaRegistry
from src.schema.inspector import SchemaInspector
from src.composer.recipe import load_recipe
from src.composer.validator import RecipeValidator
from src.composer.merger import DatasetMerger


def cmd_data_reset(args):
    """
    清空 / 重建 data/ 目錄（用來把歷史快取污染、錯 dtype 的 parquet、舊輸出全部清掉）。

    預設會刪除：
    - data/raw_parquet
    - data/raw_zips
    - data/downloads
    - data/outputs/datasets   （除非 --keep-outputs）

    注意：不會刪除 catalog.db / schema.db（它們在專案根目錄）。
    """
    root = Path(args.root)
    data_dir = root / "data"
    if not data_dir.exists():
        print(f"data/ 不存在：{data_dir}")
        return

    targets = [
        data_dir / "raw_parquet",
        data_dir / "raw_zips",
        data_dir / "downloads",
    ]
    if not args.keep_outputs:
        targets.append(data_dir / "outputs" / "datasets")

    print("即將刪除並重建以下資料夾：")
    for t in targets:
        print(f"  - {t}")
    if not args.yes:
        print("\n安全起見，請加上 --yes 才會真的執行刪除。")
        return

    for t in targets:
        try:
            if t.exists():
                shutil.rmtree(t, ignore_errors=False)
            t.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"刪除/重建失敗：{t} err={e}")
            raise

    # ensure base dirs exist
    (data_dir / "outputs" / "datasets").mkdir(parents=True, exist_ok=True)

    print("data/ 重置完成。")


def cmd_catalog_build(args):
    """建立資料目錄"""
    print(f"Building catalog for symbols: {args.symbols}")
    print(f"Date range: {args.start} to {args.end}")
    
    db = init_database(args.db)
    builder = CatalogBuilder(db, market=args.market)
    
    builder.build_catalog(
        symbols=args.symbols,
        start_date=args.start,
        end_date=args.end,
        dataset_types=args.dataset_types,
        intervals=args.intervals,
        cadence=args.cadence
    )
    
    # 更新 coverage
    for symbol in args.symbols:
        for ds_type in (args.dataset_types or builder.DATASET_TYPES):
            if ds_type in builder.INTERVAL_DATASETS:
                for interval in (args.intervals or builder.INTERVALS):
                    builder.update_coverage(ds_type, symbol, interval, cadence=args.cadence)
            else:
                builder.update_coverage(ds_type, symbol, None, cadence=args.cadence)
    
    print("Catalog built successfully!")


def cmd_coverage(args):
    """查看 coverage"""
    db = CatalogDB(args.db)
    analyzer = CoverageAnalyzer(db)
    
    if args.symbol and args.dataset_type:
        # 單一查詢
        coverage = analyzer.get_symbol_coverage(
            args.symbol, args.dataset_type, args.interval
        )
        if coverage:
            print(f"\nCoverage for {args.symbol} {args.dataset_type}:")
            print(f"  Start: {coverage['start_date']}")
            print(f"  End: {coverage['end_date']}")
            print(f"  Files: {coverage['num_files']}")
            print(f"  Missing: {coverage['num_missing']}")
            if coverage['missing_dates']:
                print(f"  Missing dates: {coverage['missing_dates'][:10]}...")
        else:
            print("No coverage found")
    else:
        # Matrix
        symbols = args.symbols or ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        matrix = analyzer.get_coverage_matrix(symbols, args.dataset_types)
        
        print("\nCoverage Matrix:")
        print("=" * 80)
        for symbol, datasets in matrix.items():
            print(f"\n{symbol}:")
            for ds_type, info in datasets.items():
                print(f"  {ds_type:20s} {info['start_date']} ~ {info['end_date']} "
                      f"({info['num_files']} files, {info['num_missing']} missing)")


def cmd_schema_inspect(args):
    """探勘 schema"""
    print(f"Inspecting schema for {args.dataset_type}")
    
    registry = SchemaRegistry(args.schema_db)
    inspector = SchemaInspector(registry)
    
    # 需要提供樣本 URL（這裡簡化，實際應該從 catalog 取得）
    if not args.sample_urls:
        print("Error: --sample-urls required")
        return
    
    schema = inspector.inspect_dataset(
        args.dataset_type,
        args.sample_urls,
        args.symbol
    )
    
    print(f"\nSchema for {args.dataset_type}:")
    print(f"  Columns: {[c['name'] for c in schema['columns']]}")
    print(f"  Primary time key: {schema['primary_time_key']}")
    print(f"  Validation: {schema['validation_status']}")


def cmd_menu_schema_build(args):
    """
    從 menu coverage 自動挑樣本 URL，建立 schema.db 的『表頭大菜單』。

    策略：
    - 對每個 dataset_type（以及 klines 類的 interval），從 coverage 找到一個 start_date
    - 用 CatalogBuilder 組 URL
    - 交給 SchemaInspector 探勘並寫入 schema.db
    """
    catalog_db = CatalogDB(args.catalog_db)
    schema_registry = SchemaRegistry(args.schema_db)
    inspector = SchemaInspector(schema_registry)

    markets = args.markets or ["futures_um", "spot"]

    # dataset sets per market
    market_datasets = {
        "spot": ["klines", "trades", "aggTrades"],
        "futures_um": ["aggTrades", "bookDepth", "bookTicker", "fundingRate", "indexPriceKlines", "klines", "markPriceKlines", "metrics", "premiumIndexKlines", "trades"],
    }
    interval_datasets = {"klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"}
    intervals = args.intervals or ["1h"]

    from src.catalog.database import Coverage

    for market in markets:
        builder = CatalogBuilder(catalog_db, market=market)
        datasets = args.dataset_types or market_datasets.get(market, [])
        print(f"\n=== Building schemas: market={market} datasets={datasets} ===")

        with catalog_db.get_session() as session:
            for ds in datasets:
                cadences = ("daily", "monthly") if args.include_monthly else ("daily",)
                if ds in interval_datasets:
                    for itv in intervals:
                        for cad in cadences:
                            row = session.query(Coverage).filter_by(
                                market=market,
                                dataset_type=ds,
                                interval=itv,
                                cadence=cad,
                            ).filter(Coverage.start_date != "").first()
                            if not row:
                                continue
                            sym = args.symbol or row.symbol
                            d = row.start_date
                            url = builder._build_url(sym, ds, d, cad, itv)
                            try:
                                inspector.inspect_dataset(ds, [url], symbol=sym)
                                print(f"OK schema: {ds} cadence={cad} interval={itv} sample={sym} {d}")
                            except Exception as e:
                                print(f"FAIL schema: {ds} cadence={cad} interval={itv} sample={url} err={e}")
                else:
                    for cad in cadences:
                        row = session.query(Coverage).filter_by(
                            market=market,
                            dataset_type=ds,
                            interval=None,
                            cadence=cad,
                        ).filter(Coverage.start_date != "").first()
                        if not row:
                            continue
                        sym = args.symbol or row.symbol
                        d = row.start_date
                        url = builder._build_url(sym, ds, d, cad, None)
                        try:
                            inspector.inspect_dataset(ds, [url], symbol=sym)
                            print(f"OK schema: {ds} cadence={cad} sample={sym} {d}")
                        except Exception as e:
                            print(f"FAIL schema: {ds} cadence={cad} sample={url} err={e}")

    print("\nMenu schema build done. Open UI -> Data Menu -> Schema section to view columns.")


def cmd_recipe_execute(args):
    """執行 recipe"""
    print(f"Loading recipe: {args.recipe}")
    
    recipe = load_recipe(args.recipe)
    print(f"Recipe: {recipe.name}")
    print(f"Symbols: {recipe.symbols}")
    print(f"Time range: {recipe.time_range}")
    
    # 驗證
    catalog_db = CatalogDB(args.catalog_db)
    schema_registry = SchemaRegistry(args.schema_db)
    validator = RecipeValidator(catalog_db, schema_registry)
    
    is_valid, errors = validator.validate(recipe)
    if not is_valid:
        print("\nValidation errors:")
        for error in errors:
            print(f"  - {error}")
        if not args.force:
            print("\nUse --force to proceed anyway")
            return
    
    # 執行
    merger = DatasetMerger(catalog_db, schema_registry, args.download_dir, args.output_dir)
    output_path = merger.execute_recipe(recipe)
    
    print(f"\nOutput saved to: {output_path}")


def cmd_menu_build(args):
    """
    建立「菜單用」coverage（不會掃每一天；以最少 HEAD 找到 start/end）
    """
    db = init_database(args.db)

    # markets
    markets = args.markets or ["spot", "futures_um"]

    # symbols: 固定 top10（或你指定）
    symbols = args.symbols or TOP10_USDT_SYMBOLS

    # dataset sets per market（可覆寫）
    market_datasets = {
        "spot": ["klines", "trades", "aggTrades"],
        "futures_um": ["aggTrades", "bookDepth", "bookTicker", "fundingRate", "indexPriceKlines", "klines", "markPriceKlines", "metrics", "premiumIndexKlines", "trades"],
    }

    # intervals（只對 klines 類）
    intervals = args.intervals or ["1m", "5m", "1h", "1d"]
    interval_datasets = set(["klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"])

    from datetime import datetime
    from sqlalchemy.orm import Session
    from src.catalog.database import Coverage

    for market in markets:
        crawler = CoverageCrawler(db, market=market)
        datasets = args.dataset_types or market_datasets.get(market, [])
        print(f"\n=== Building menu coverage: market={market} symbols={len(symbols)} datasets={datasets} ===")

        with db.get_session() as session:
            for sym in symbols:
                for ds in datasets:
                    if ds in interval_datasets:
                        for itv in intervals:
                            for cadence in ("daily", "monthly") if args.include_monthly else ("daily",):
                                res = crawler.find_range(sym, ds, cadence=cadence, interval=itv)
                                _upsert_coverage_row(session, market, ds, sym, itv, cadence, res)
                    else:
                        for cadence in ("daily", "monthly") if args.include_monthly else ("daily",):
                            res = crawler.find_range(sym, ds, cadence=cadence, interval=None)
                            _upsert_coverage_row(session, market, ds, sym, None, cadence, res)
                session.commit()

    print("\nMenu coverage build done. You can now open UI and browse the menu.")


def _upsert_coverage_row(session, market: str, dataset_type: str, symbol: str, interval, cadence: str, res):
    from src.catalog.database import Coverage
    row = session.query(Coverage).filter_by(
        market=market,
        dataset_type=dataset_type,
        symbol=symbol,
        interval=interval,
        cadence=cadence,
    ).first()

    if not res.exists:
        # keep row but mark empty range
        if row:
            row.start_date = ""
            row.end_date = ""
            row.num_files = 0
            row.num_missing = 0
            row.missing_date_list = []
            row.total_size_estimate = None
        else:
            row = Coverage(
                market=market,
                dataset_type=dataset_type,
                symbol=symbol,
                interval=interval,
                cadence=cadence,
                start_date="",
                end_date="",
                num_files=0,
                num_missing=0,
                missing_date_list=[],
                total_size_estimate=None
            )
            session.add(row)
        return

    # store cadence in start/end strings (monthly uses YYYY-MM)
    start = res.start or ""
    end = res.end or ""

    if row:
        row.start_date = start
        row.end_date = end
    else:
        row = Coverage(
            market=market,
            dataset_type=dataset_type,
            symbol=symbol,
            interval=interval,
            cadence=cadence,
            start_date=start,
            end_date=end,
        )
        session.add(row)


def main():
    parser = argparse.ArgumentParser(description="Binance Vision 資料統整系統 CLI")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # data reset
    parser_reset = subparsers.add_parser("data-reset", help="清空並重建 data/（清掉 raw cache / downloads / outputs）")
    parser_reset.add_argument("--root", default=".", help="專案根目錄（預設：.）")
    parser_reset.add_argument("--keep-outputs", action="store_true", help="保留 data/outputs/datasets（不刪輸出）")
    parser_reset.add_argument("--yes", action="store_true", help="真的執行刪除（沒有此旗標只顯示將刪除項目）")
    parser_reset.set_defaults(func=cmd_data_reset)
    
    # catalog build
    parser_build = subparsers.add_parser("catalog-build", help="建立資料目錄")
    parser_build.add_argument("--symbols", nargs="+", required=True)
    parser_build.add_argument("--start", required=True)
    parser_build.add_argument("--end", required=True)
    parser_build.add_argument("--dataset-types", nargs="+")
    parser_build.add_argument("--intervals", nargs="+")
    parser_build.add_argument("--cadence", default="daily", choices=["daily", "monthly"])
    parser_build.add_argument("--market", default="futures_um", choices=["futures_um", "spot"])
    parser_build.add_argument("--db", default="catalog.db")
    parser_build.set_defaults(func=cmd_catalog_build)
    
    # coverage
    parser_cov = subparsers.add_parser("coverage", help="查看 coverage")
    parser_cov.add_argument("--symbol")
    parser_cov.add_argument("--dataset-type")
    parser_cov.add_argument("--interval")
    parser_cov.add_argument("--symbols", nargs="+")
    parser_cov.add_argument("--dataset-types", nargs="+")
    parser_cov.add_argument("--db", default="catalog.db")
    parser_cov.set_defaults(func=cmd_coverage)
    
    # schema inspect
    parser_schema = subparsers.add_parser("schema-inspect", help="探勘 schema")
    parser_schema.add_argument("--dataset-type", required=True)
    parser_schema.add_argument("--sample-urls", nargs="+", required=True)
    parser_schema.add_argument("--symbol", default="BTCUSDT")
    parser_schema.add_argument("--schema-db", default="schema.db")
    parser_schema.set_defaults(func=cmd_schema_inspect)
    
    # recipe execute
    parser_recipe = subparsers.add_parser("recipe-execute", help="執行 recipe")
    parser_recipe.add_argument("--recipe", required=True)
    parser_recipe.add_argument("--catalog-db", default="catalog.db")
    parser_recipe.add_argument("--schema-db", default="schema.db")
    parser_recipe.add_argument("--download-dir", default="data/downloads")
    parser_recipe.add_argument("--output-dir", default="data/outputs")
    parser_recipe.add_argument("--force", action="store_true")
    parser_recipe.set_defaults(func=cmd_recipe_execute)

    # menu build (top10 + spot+futures_um)
    parser_menu = subparsers.add_parser("menu-build", help="建立『大菜單』coverage（前十大、spot+futures_um）")
    parser_menu.add_argument("--db", default="catalog.db")
    parser_menu.add_argument("--markets", nargs="+", choices=["spot", "futures_um"])
    parser_menu.add_argument("--symbols", nargs="+", help="預設為固定前十大 USDT 標的")
    parser_menu.add_argument("--dataset-types", nargs="+", help="覆寫 market 預設 dataset 列表")
    parser_menu.add_argument("--intervals", nargs="+", help="klines 類的 interval（預設：1m 5m 1h 1d）")
    parser_menu.add_argument("--include-monthly", action="store_true", help="同時盤點 monthly 檔（若存在）")
    parser_menu.set_defaults(func=cmd_menu_build)

    # menu schema build (auto sample from coverage)
    parser_menu_schema = subparsers.add_parser("menu-schema-build", help="從 menu coverage 自動抽樣建立 schema.db（表頭大菜單）")
    parser_menu_schema.add_argument("--catalog-db", default="catalog.db")
    parser_menu_schema.add_argument("--schema-db", default="schema.db")
    parser_menu_schema.add_argument("--include-monthly", action="store_true", help="同時用 monthly 抽樣探勘 schema（若 coverage 有）")
    parser_menu_schema.add_argument("--markets", nargs="+", choices=["spot", "futures_um"])
    parser_menu_schema.add_argument("--dataset-types", nargs="+")
    parser_menu_schema.add_argument("--intervals", nargs="+", help="klines 類 sample interval（預設：1h）")
    parser_menu_schema.add_argument("--symbol", help="強制用此 symbol 抽樣（預設用 coverage row 的 symbol）")
    parser_menu_schema.set_defaults(func=cmd_menu_schema_build)
    
    args = parser.parse_args()
    
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

