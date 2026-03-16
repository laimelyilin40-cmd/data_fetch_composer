"""
固定前十大名單（可自行修改）

我們先用「USDT 計價」的常見 Top10 近年主流標的。
若某些標的在特定 market（spot / futures_um）不存在，crawler 會顯示為 unavailable。
"""

TOP10_USDT_SYMBOLS: list[str] = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "TRXUSDT",
    "AVAXUSDT",
    "TONUSDT",
]


