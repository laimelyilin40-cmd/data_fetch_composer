"""
Coverage Crawler：針對 Vision 以最少 HEAD 次數找出 start/end（不掃每一天）
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional, Tuple, Dict, Any, List

import requests
import calendar

from .builder import CatalogBuilder
from .database import CatalogDB


def _today_utc_date() -> date:
    return datetime.utcnow().date()


def _parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _parse_ym(s: str) -> date:
    return datetime.strptime(s, "%Y-%m").date()


def _fmt_ymd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _fmt_ym(d: date) -> str:
    return d.strftime("%Y-%m")


def _add_months(d: date, months: int) -> date:
    # safe month add for first day of month
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    return date(y, m, 1)


def _months_between(a: date, b: date) -> int:
    """How many whole months to move from a to reach b (a and b should be first-of-month)."""
    return (b.year - a.year) * 12 + (b.month - a.month)


def _days_between(a: date, b: date) -> int:
    return (b - a).days


@dataclass(frozen=True)
class CoverageResult:
    exists: bool
    start: Optional[str] = None
    end: Optional[str] = None
    sample_start_meta: Optional[Dict[str, Any]] = None
    sample_end_meta: Optional[Dict[str, Any]] = None


class CoverageCrawler:
    """
    以少量 HEAD 請求找出每個 (market, symbol, dataset_type, cadence, interval) 的 start/end。

    注意：
    - 這裡主要是做「菜單」用的 coverage（範圍），不會把每一天檔案都寫入 files 表。
    - 需要更細的缺口分析再另外做「按需掃描」。
    """

    def __init__(self, catalog_db: CatalogDB, market: str):
        self.catalog_db = catalog_db
        self.market = market
        self.builder = CatalogBuilder(catalog_db, market=market)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Binance-Vision-CoverageCrawler/1.0"})
        # 給 CLI 做「是否網路異常」提示用
        self.network_error_count = 0
        self.last_network_error: Optional[str] = None

    def head(self, url: str) -> Tuple[bool, Dict[str, Any]]:
        try:
            r = self.session.head(url, timeout=20, allow_redirects=True)
            ok = r.status_code == 200
            meta = {
                "status": r.status_code,
                "content_length": r.headers.get("Content-Length"),
                "etag": r.headers.get("ETag"),
                "last_modified": r.headers.get("Last-Modified"),
            }
            return ok, meta
        except Exception as e:
            self.network_error_count += 1
            self.last_network_error = str(e)
            return False, {"error": str(e)}

    def find_range(
        self,
        symbol: str,
        dataset_type: str,
        cadence: str,
        interval: Optional[str] = None,
        min_date: Optional[date] = None,
        max_date: Optional[date] = None,
    ) -> CoverageResult:
        if cadence not in ("daily", "monthly"):
            raise ValueError("cadence must be daily/monthly")

        if min_date is None:
            # Binance 早期可追到 2017，先用保守下界
            min_date = date(2017, 1, 1)
        if max_date is None:
            max_date = _today_utc_date()

        if cadence == "monthly":
            # 先找最後一個存在的月份（避免 max_date 當月尚未上傳導致錯判）
            end, end_meta = self._find_last_month(symbol, dataset_type, interval, min_date, max_date)
            if not end:
                return CoverageResult(False)
            # 用 end 當作新的上界再找 start（避免 step overshoot 到不存在的最末月）
            end_dt = _parse_ym(end)
            start, start_meta = self._find_first_month(symbol, dataset_type, interval, min_date, end_dt)
            if not start:
                return CoverageResult(False)
            return CoverageResult(True, start=start, end=end, sample_start_meta=start_meta, sample_end_meta=end_meta)

        # daily
        # 先找最後一個存在的日期（避免今天/最近幾天尚未上傳導致錯判）
        end, end_meta = self._find_last_day(symbol, dataset_type, interval, min_date, max_date)
        if not end:
            # 有些 dataset（如 bookTicker）daily 可能非常稀疏，exponential probe 會「跳過」有效區間
            end, end_meta = self._find_last_day_monthscan(symbol, dataset_type, interval, min_date, max_date)
        if not end:
            return CoverageResult(False)

        end_dt = _parse_ymd(end)
        # 用 end_dt 當上界再找 start
        start, start_meta = self._find_first_day(symbol, dataset_type, interval, min_date, end_dt)
        if not start:
            start, start_meta = self._find_first_day_monthscan(symbol, dataset_type, interval, min_date, end_dt)
        if not start:
            return CoverageResult(False)

        return CoverageResult(True, start=start, end=end, sample_start_meta=start_meta, sample_end_meta=end_meta)

    def _url_for(self, symbol: str, dataset_type: str, cadence: str, d_str: str, interval: Optional[str]) -> str:
        return self.builder._build_url(symbol=symbol, dataset_type=dataset_type, date=d_str, cadence=cadence, interval=interval)

    # ---------- daily search ----------
    def _exists_day(self, symbol: str, dataset_type: str, interval: Optional[str], d: date) -> Tuple[bool, Dict[str, Any]]:
        u = self._url_for(symbol, dataset_type, "daily", _fmt_ymd(d), interval)
        return self.head(u)

    def _find_first_day(self, symbol: str, dataset_type: str, interval: Optional[str], lo: date, hi: date) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        # exponential forward to find a True
        step = 1
        cur = lo
        ok, meta = self._exists_day(symbol, dataset_type, interval, cur)
        if ok:
            # binary search backwards range not needed
            return _fmt_ymd(cur), meta

        while cur <= hi:
            remaining = _days_between(cur, hi)
            if remaining <= 0:
                break
            step_to_use = min(step, remaining)
            nxt = cur + timedelta(days=step_to_use)
            ok, meta = self._exists_day(symbol, dataset_type, interval, nxt)
            if ok:
                # binary search in (cur, nxt]
                return self._binary_first_day(symbol, dataset_type, interval, cur + timedelta(days=1), nxt)
            cur = nxt
            step *= 2
        return None, None

    def _binary_first_day(self, symbol: str, dataset_type: str, interval: Optional[str], lo: date, hi: date) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        ans = None
        ans_meta = None
        while lo <= hi:
            mid = lo + (hi - lo) // 2
            ok, meta = self._exists_day(symbol, dataset_type, interval, mid)
            if ok:
                ans = mid
                ans_meta = meta
                hi = mid - timedelta(days=1)
            else:
                lo = mid + timedelta(days=1)
        if ans is None:
            return None, None
        return _fmt_ymd(ans), ans_meta

    def _find_last_day(self, symbol: str, dataset_type: str, interval: Optional[str], lo: date, hi: date) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        # exponential backward to find a True
        step = 1
        cur = hi
        ok, meta = self._exists_day(symbol, dataset_type, interval, cur)
        if ok:
            return _fmt_ymd(cur), meta

        while cur >= lo:
            remaining = _days_between(lo, cur)
            if remaining <= 0:
                break
            step_to_use = min(step, remaining)
            nxt = cur - timedelta(days=step_to_use)
            ok, meta = self._exists_day(symbol, dataset_type, interval, nxt)
            if ok:
                # binary search in [nxt, cur)
                return self._binary_last_day(symbol, dataset_type, interval, nxt, cur - timedelta(days=1))
            cur = nxt
            step *= 2
        return None, None

    def _binary_last_day(self, symbol: str, dataset_type: str, interval: Optional[str], lo: date, hi: date) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        ans = None
        ans_meta = None
        while lo <= hi:
            mid = lo + (hi - lo) // 2
            ok, meta = self._exists_day(symbol, dataset_type, interval, mid)
            if ok:
                ans = mid
                ans_meta = meta
                lo = mid + timedelta(days=1)
            else:
                hi = mid - timedelta(days=1)
        if ans is None:
            return None, None
        return _fmt_ymd(ans), ans_meta

    # ---------- daily fallback (month scan) ----------
    def _month_days(self, d: date) -> int:
        return calendar.monthrange(d.year, d.month)[1]

    def _month_probe_days(self, d: date) -> List[int]:
        # 1, 8, 15, 22, last day
        last = self._month_days(d)
        days = [1, 8, 15, 22, last]
        # unique & valid
        out = []
        for x in days:
            if 1 <= x <= last and x not in out:
                out.append(x)
        return out

    def _month_has_any_daily(self, symbol: str, dataset_type: str, interval: Optional[str], month: date) -> bool:
        for day in self._month_probe_days(month):
            ok, _ = self._exists_day(symbol, dataset_type, interval, date(month.year, month.month, day))
            if ok:
                return True
        return False

    def _find_last_day_monthscan(self, symbol: str, dataset_type: str, interval: Optional[str], lo: date, hi: date) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        # scan months backward to find a month containing any file, then scan days backward inside it
        m = date(hi.year, hi.month, 1)
        start_m = date(lo.year, lo.month, 1)

        while m >= start_m:
            if self._month_has_any_daily(symbol, dataset_type, interval, m):
                # scan days backward within this month
                for day in range(self._month_days(m), 0, -1):
                    d = date(m.year, m.month, day)
                    if d < lo or d > hi:
                        continue
                    ok, meta = self._exists_day(symbol, dataset_type, interval, d)
                    if ok:
                        return _fmt_ymd(d), meta
            # prev month
            m = _add_months(m, -1)
        return None, None

    def _find_first_day_monthscan(self, symbol: str, dataset_type: str, interval: Optional[str], lo: date, hi: date) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        # scan months forward to find a month containing any file, then scan days forward inside it
        m = date(lo.year, lo.month, 1)
        end_m = date(hi.year, hi.month, 1)

        while m <= end_m:
            if self._month_has_any_daily(symbol, dataset_type, interval, m):
                for day in range(1, self._month_days(m) + 1):
                    d = date(m.year, m.month, day)
                    if d < lo or d > hi:
                        continue
                    ok, meta = self._exists_day(symbol, dataset_type, interval, d)
                    if ok:
                        return _fmt_ymd(d), meta
            m = _add_months(m, 1)
        return None, None

    # ---------- monthly search ----------
    def _exists_month(self, symbol: str, dataset_type: str, interval: Optional[str], d: date) -> Tuple[bool, Dict[str, Any]]:
        u = self._url_for(symbol, dataset_type, "monthly", _fmt_ym(d), interval)
        return self.head(u)

    def _find_first_month(self, symbol: str, dataset_type: str, interval: Optional[str], lo: date, hi: date) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        # normalize to first of month
        cur = date(lo.year, lo.month, 1)
        end = date(hi.year, hi.month, 1)
        step = 1
        ok, meta = self._exists_month(symbol, dataset_type, interval, cur)
        if ok:
            return _fmt_ym(cur), meta

        while cur <= end:
            max_step = _months_between(cur, end)
            if max_step <= 0:
                break
            step_to_use = min(step, max_step)
            nxt = _add_months(cur, step_to_use)
            ok, meta = self._exists_month(symbol, dataset_type, interval, nxt)
            if ok:
                # binary-like by month within (cur, nxt]
                return self._binary_first_month(symbol, dataset_type, interval, _add_months(cur, 1), nxt)
            cur = nxt
            step *= 2
        return None, None

    def _binary_first_month(self, symbol: str, dataset_type: str, interval: Optional[str], lo: date, hi: date) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        ans = None
        ans_meta = None
        while lo <= hi:
            # pick mid month by counting months
            months = (hi.year - lo.year) * 12 + (hi.month - lo.month)
            mid = _add_months(lo, months // 2)
            ok, meta = self._exists_month(symbol, dataset_type, interval, mid)
            if ok:
                ans = mid
                ans_meta = meta
                # hi = mid - 1 month
                hi = _add_months(mid, -1)
            else:
                lo = _add_months(mid, 1)
        if ans is None:
            return None, None
        return _fmt_ym(ans), ans_meta

    def _find_last_month(self, symbol: str, dataset_type: str, interval: Optional[str], lo: date, hi: date) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        cur = date(hi.year, hi.month, 1)
        start = date(lo.year, lo.month, 1)
        step = 1
        ok, meta = self._exists_month(symbol, dataset_type, interval, cur)
        if ok:
            return _fmt_ym(cur), meta

        while cur >= start:
            max_step = _months_between(start, cur)
            if max_step <= 0:
                break
            step_to_use = min(step, max_step)
            nxt = _add_months(cur, -step_to_use)
            ok, meta = self._exists_month(symbol, dataset_type, interval, nxt)
            if ok:
                return self._binary_last_month(symbol, dataset_type, interval, nxt, _add_months(cur, -1))
            cur = nxt
            step *= 2
        return None, None

    def _binary_last_month(self, symbol: str, dataset_type: str, interval: Optional[str], lo: date, hi: date) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        ans = None
        ans_meta = None
        while lo <= hi:
            months = (hi.year - lo.year) * 12 + (hi.month - lo.month)
            mid = _add_months(lo, months // 2)
            ok, meta = self._exists_month(symbol, dataset_type, interval, mid)
            if ok:
                ans = mid
                ans_meta = meta
                lo = _add_months(mid, 1)
            else:
                hi = _add_months(mid, -1)
        if ans is None:
            return None, None
        return _fmt_ym(ans), ans_meta


