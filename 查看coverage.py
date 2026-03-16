"""
簡化的 Coverage 查看腳本
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.catalog.database import CatalogDB
from src.catalog.coverage import CoverageAnalyzer

def main():
    print("=" * 60)
    print("Coverage 查看工具")
    print("=" * 60)
    
    db_path = "catalog.db"
    
    if not Path(db_path).exists():
        print(f"\n❌ 找不到資料庫：{db_path}")
        print("請先執行 setup_catalog.py 建立資料目錄")
        return 1
    
    db = CatalogDB(db_path)
    analyzer = CoverageAnalyzer(db)
    
    # 預設查詢
    symbols_input = input("\n輸入交易對（用空格分隔，預設: BTCUSDT ETHUSDT）: ").strip()
    symbols = symbols_input.split() if symbols_input else ["BTCUSDT", "ETHUSDT"]
    
    dataset_types_input = input("輸入資料集類型（用空格分隔，留空=全部）: ").strip()
    dataset_types = dataset_types_input.split() if dataset_types_input else None
    
    print("\n查詢 Coverage...")
    
    matrix = analyzer.get_coverage_matrix(symbols, dataset_types)
    
    if not matrix:
        print("\n❌ 沒有找到資料")
        return 1
    
    print("\n" + "=" * 60)
    print("Coverage Matrix")
    print("=" * 60)
    
    for symbol, datasets in matrix.items():
        print(f"\n{symbol}:")
        for ds_type, info in datasets.items():
            print(f"  {ds_type:20s} {info['start_date']} ~ {info['end_date']}")
            print(f"    Files: {info['num_files']}, Missing: {info['num_missing']}")
            if info['missing_dates']:
                missing_preview = info['missing_dates'][:5]
                print(f"    Missing dates: {missing_preview}{'...' if len(info['missing_dates']) > 5 else ''}")
            size_mb = info['total_size'] / 1024 / 1024 if info['total_size'] else 0
            print(f"    Size: {size_mb:.2f} MB")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

