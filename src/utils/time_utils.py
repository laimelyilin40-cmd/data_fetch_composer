"""
時間工具函數
"""

from datetime import datetime, timedelta
from typing import Tuple, List
from dateutil.parser import parse as parse_date


def parse_date_range(start: str, end: str) -> Tuple[datetime, datetime]:
    """解析日期範圍字串"""
    start_dt = parse_date(start) if isinstance(start, str) else start
    end_dt = parse_date(end) if isinstance(end, str) else end
    return start_dt, end_dt


def date_to_str(dt: datetime, format: str = "%Y-%m-%d") -> str:
    """日期轉字串"""
    return dt.strftime(format)


def str_to_date(date_str: str, format: str = "%Y-%m-%d") -> datetime:
    """字串轉日期"""
    return datetime.strptime(date_str, format)


def generate_date_list(start: datetime, end: datetime, cadence: str = "daily") -> List[str]:
    """產生日期列表"""
    dates = []
    current = start
    
    if cadence == "daily":
        while current <= end:
            dates.append(date_to_str(current))
            current += timedelta(days=1)
    elif cadence == "monthly":
        while current <= end:
            dates.append(date_to_str(current, "%Y-%m"))
            # 移到下個月
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
    
    return dates

