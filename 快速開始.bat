@echo off
chcp 65001 >nul
echo ========================================
echo Binance Vision 資料統整系統
echo ========================================
echo.
echo 請選擇操作：
echo.
echo 1. 建立資料目錄（Catalog）
echo 2. 查看 Coverage
echo 3. 啟動 UI
echo 4. 退出
echo.
set /p choice=請輸入選項 (1-4): 

if "%choice%"=="1" (
    echo.
    echo 啟動資料目錄建立工具...
    python setup_catalog.py
    pause
) else if "%choice%"=="2" (
    echo.
    echo 啟動 Coverage 查看工具...
    python 查看coverage.py
    pause
) else if "%choice%"=="3" (
    echo.
    echo 啟動 UI...
    python run_ui.py
    pause
) else if "%choice%"=="4" (
    exit
) else (
    echo 無效選項
    pause
)

