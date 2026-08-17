"""A股交易日判断脚本（供计划任务 bat 调用）

逻辑：
1. 每次运行前先尝试升级 chinese_calendar 包 —— 节假日数据随年度发布，只有最新版才有当年数据
2. 判断指定日期（默认今天）是否为 A 股交易日：
   - 周一至周五（weekday < 5）
   - 且非法定节假日/调休休市（not is_holiday）
   - 注意：不能用 is_workday —— 调休补班的周六日 is_workday=True 但股市不开市
3. 退出码：0=交易日（继续执行下载），1=非交易日（跳过）

用法:
    python check_trading_day.py
    python check_trading_day.py --date 2026-10-01   # 测试指定日期

返回:
    exit 0: 交易日
    exit 1: 非交易日
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, datetime


def upgrade_package() -> None:
    """静默升级 chinese_calendar，失败不阻塞（无网时沿用旧包数据）"""
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--upgrade", "--quiet",
             "--disable-pip-version-check", "chinese_calendar"],
            timeout=60,
            check=False,
            capture_output=True,
        )
    except Exception:
        pass


def is_trading_day(d: date) -> bool:
    """A股交易日：周一至周五 且 非法定节假日/调休休市"""
    if d.weekday() >= 5:
        return False
    try:
        import chinese_calendar as cc
        return not cc.is_holiday(d)
    except Exception:
        # 包数据未覆盖该年份等异常：退回仅排除周末（保守可跑）
        return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A股交易日判断")
    parser.add_argument("--date", type=str, default="", help="YYYY-MM-DD，默认今天")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    upgrade_package()
    if args.date:
        today = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        today = datetime.now().date()
    if is_trading_day(today):
        print(f"[check_trading_day] {today} is trading day, proceed")
        return 0
    print(f"[check_trading_day] {today} is NOT trading day, skip")
    return 1


if __name__ == "__main__":
    sys.exit(main())