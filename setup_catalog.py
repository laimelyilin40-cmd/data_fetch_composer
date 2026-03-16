"""
簡化的 Catalog 建立腳本
避免 CLI 參數複雜性
"""

import sys
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.insert(0, str(Path(__file__).parent))

from src.catalog.database import init_database, CatalogDB
from src.catalog.builder import CatalogBuilder

def main():
    print("=" * 60)
    print("Binance Vision 資料目錄建立工具")
    print("=" * 60)
    
    # 預設參數（可以修改）
    symbols = ["BTCUSDT", "ETHUSDT"]
    start_date = "2023-01-01"
    end_date = "2023-01-31"
    dataset_types = ["klines", "metrics"]
    intervals = ["1h"]
    cadence = "daily"
    db_path = "catalog.db"
    
    print(f"\n設定：")
    print(f"  交易對: {symbols}")
    print(f"  日期範圍: {start_date} ~ {end_date}")
    print(f"  資料集類型: {dataset_types}")
    print(f"  Interval: {intervals}")
    print(f"  資料庫: {db_path}")
    
    response = input("\n是否使用預設設定？(Y/n): ").strip().lower()
    
    if response == 'n':
        symbols_input = input("輸入交易對（用空格分隔，例如：BTCUSDT ETHUSDT）: ").strip()
        symbols = symbols_input.split() if symbols_input else symbols
        
        start_date = input(f"開始日期 (預設: {start_date}): ").strip() or start_date
        end_date = input(f"結束日期 (預設: {end_date}): ").strip() or end_date
        
        dataset_types_input = input(f"資料集類型（用空格分隔，預設: {' '.join(dataset_types)}）: ").strip()
        dataset_types = dataset_types_input.split() if dataset_types_input else dataset_types
        
        intervals_input = input(f"Interval（用空格分隔，預設: {' '.join(intervals)}）: ").strip()
        intervals = intervals_input.split() if intervals_input else intervals
    
    print("\n開始建立資料目錄...")
    
    try:
        # 初始化資料庫
        db = init_database(db_path)
        builder = CatalogBuilder(db)
        
        # 建立 catalog
        builder.build_catalog(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            dataset_types=dataset_types,
            intervals=intervals,
            cadence=cadence
        )
        
        # 更新 coverage
        print("\n更新 Coverage 摘要...")
        for symbol in symbols:
            for ds_type in dataset_types:
                if ds_type in builder.INTERVAL_DATASETS:
                    for interval in intervals:
                        builder.update_coverage(symbol, ds_type, interval)
                else:
                    builder.update_coverage(symbol, ds_type, None)
        
        print(f"\n✅ 完成！資料目錄已建立：{db_path}")
        print("\n下一步：")
        print("  1. 查看 coverage: python -m cli.main coverage --symbols BTCUSDT")
        print("  2. 啟動 UI: streamlit run ui/app.py")
        
    except Exception as e:
        print(f"\n❌ 錯誤：{e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

