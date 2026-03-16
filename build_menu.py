"""
一鍵建立「大菜單」：
1) menu-build：盤點前十大（spot + futures_um）各 dataset 的 start/end（含 monthly）
2) menu-schema-build：自動抽樣建立 schema.db

用法：
  python build_menu.py
"""

import subprocess
import sys


def run(cmd: list[str]) -> None:
    print("\n> " + " ".join(cmd))
    subprocess.check_call(cmd)


def main() -> int:
    # 1) coverage menu
    run([sys.executable, "-m", "cli.main", "menu-build", "--include-monthly"])

    # 2) schema menu
    run([sys.executable, "-m", "cli.main", "menu-schema-build", "--intervals", "1h"])

    print("\nDone. Now run: streamlit run ui/app.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


