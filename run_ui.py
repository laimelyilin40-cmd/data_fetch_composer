"""
簡化的 UI 啟動腳本
"""

import subprocess
import sys
from pathlib import Path

def main():
    ui_path = Path(__file__).parent / "ui" / "app.py"
    
    if not ui_path.exists():
        print(f"❌ 找不到 UI 檔案：{ui_path}")
        return 1
    
    print("=" * 60)
    print("啟動 Binance Vision UI")
    print("=" * 60)
    port = 8511
    if len(sys.argv) >= 2:
        try:
            port = int(sys.argv[1])
        except Exception:
            print(f"⚠️ 參數 port 無效：{sys.argv[1]!r}，將使用預設 {port}")

    print(f"\n正在啟動 Streamlit...")
    print(f"瀏覽器會自動打開 http://127.0.0.1:{port}")
    print("\n按 Ctrl+C 停止服務\n")
    
    try:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                str(ui_path),
                "--server.address",
                "127.0.0.1",
                "--server.port",
                str(port),
            ]
        )
    except KeyboardInterrupt:
        print("\n\n已停止 UI 服務")
        return 0
    except Exception as e:
        print(f"\n❌ 錯誤：{e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())

