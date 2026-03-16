import sys
from pathlib import Path
import polars as pl

# allow running from anywhere (Windows 路徑含中文時 cwd 可能失敗)
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.features.formula_engine import ColumnSpec, FormulaEngine


def main():
    p = "data/outputs/datasets/toutou2_rebuild_tsfix/toutou2_rebuild_tsfix.parquet"
    lf = pl.scan_parquet(p).sort("ts")
    cols = lf.collect_schema().names()
    eng = FormulaEngine(cols)

    specs = [
        ColumnSpec(name="btc_ret_1", formula="LOGRET(BTCUSDT_klines_1m_close, 1)"),
        ColumnSpec(name="cs_z", formula="XS_ZSCORE(BTCUSDT_klines_1m_close, COLS('.*_klines_1m_close$'))"),
        ColumnSpec(name="top2_mean", formula="ROW_TOPK_MEAN(COLS('.*_klines_1m_close$'), 2)"),
        ColumnSpec(name="rz", formula="ROLL_ZSCORE(BTCUSDT_klines_1m_close, 60)"),
        ColumnSpec(name="wz", formula="ZSCORE(WINSORIZE(btc_ret_1, 0.01, 0.99))"),
    ]

    lf2, errs = eng.apply_specs(lf, specs)
    print("errors:", errs)
    df = lf2.select(["btc_ret_1", "cs_z", "top2_mean", "rz", "wz", "ts"]).head(3).collect()
    print(df.to_dicts())


if __name__ == "__main__":
    main()


