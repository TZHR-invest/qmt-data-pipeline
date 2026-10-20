"""
1m K-line 增量更新 + 按日 Parquet 导出（多进程版）

用法： python update_all_1m_mp.py
      python update_all_1m_mp.py --workers 4

写入日级 parquet 文件，速度快（只写不读）。
月度合并由 consolidate_1m_to_monthly.py 完成。
"""
import sys, os, argparse
from datetime import datetime, timedelta
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
# 2026-09-15: mini 退役后 miniquote(58610) 消失，行情改走大QMT 桥。
# 必须插在 _WORKSPACE 之后：G:\qmt_projects\xtquant 是旧 SDK 副本(与 venv 内那份
# 逐字节相同)，会遮蔽桥的影子包；spawn 子进程会重跑本模块体，故对子进程同样生效。
sys.path.insert(0, r"C:\bridge-client")
sys.path.insert(0, r"C:\bridge-client\bridge\src")
import pandas as pd
from multiprocessing import Pool


def process_stock(args):
    """单个股票的 1m 下载 + 写入当日 parquet"""
    code, out_dir_base, data_dir, today_ymd, today_dash = args

    import os as _os
    _os.environ["OMP_NUM_THREADS"] = "1"
    import xtquant.xtdata as xtdata
    import pyarrow as pa
    import pyarrow.parquet as pq

    xtdata.data_dir = data_dir

    code_dir = _os.path.join(out_dir_base, code.replace(".", "_"))
    day_file = _os.path.join(code_dir, f"{today_dash}.parquet")

    # 跳过已完成的
    if _os.path.exists(day_file) and _os.path.getsize(day_file) > 0:
        return ("skip", code)

    # 先 download 确保数据被拉到本地（get_market_data_ex 对当天数据不会自动从服务器拉取）
    import time as _time
    # ── 2026-10-14 读优先（read-first）—— 同 tick，依据见其注释与管道档 §13.5 ──
    def _has_vol(f):
        """整段 volume==0 视作"无有效数据"（fill 痕迹）。取不到列时保守放行。"""
        try:
            return f is not None and len(f) > 0 and int(f["volume"].sum()) != 0
        except Exception:
            return f is not None and len(f) > 0

    def _read_1m():
        # 2026-10-20: fill_data=False 是硬要求 —— 默认 True 会在取不到数据时
        # 前向填充 241 根 open=high=low=close、volume=0 的"平盘假 K 线"，
        # 10-15/10-19 两天全市场就是这样被写成假数据的（行数/日期全对，
        # 只看行数的判据全部失明）。
        _raw = xtdata.get_market_data_ex(
            field_list=[], stock_list=[code], period="1m",
            start_time=today_ymd, end_time=today_ymd, count=-1,
            fill_data=False,
        )
        return _raw.get(code) if _raw else None

    _rf_on = os.environ.get("PIPE_READ_FIRST", "1") not in ("0", "false", "no", "off")
    t_rf = _time.time()
    frame = None
    rf_exc = ""
    if _rf_on:
        try:
            frame = _read_1m()
        except Exception as exc:
            rf_exc = "%s: %s" % (type(exc).__name__, str(exc)[:200].replace("\n", " "))

    dl_note = ""
    if _rf_on and not rf_exc and _has_vol(frame):
        print("[1m-read-first] %s %.2fs rows=%d" % (code, _time.time() - t_rf, len(frame)))
    # ── 2026-10-15 事故回退：**空读必须回落 download** ─────────────────
    # 原实现把「read 返回 0 行」当成合法空（停牌/退市）并直接 return empty、
    # 跳过 download。当天实证（001338.SZ）：read-only=0 行 →
    # download_history_data 1.98s → 再 read=3708 行 ⇒ **当天数据必须先 download**
    # （原注释「必须显式 download，get_market_data 不会自动拉当天数据」是对的）。
    # 后果：10-15 tick 归档 5574→509 只（exit=1）。
    # 故：删除该分支，让空读落回下面的 download+read；read-first 的收益只在
    # 「数据已在本地」时保留（1m 实测 720s→463s，不受影响）。
    else:
        t_dl = _time.time()
        try:
            xtdata.download_history_data(code, period="1m", start_time=today_ymd,
                                         end_time=today_ymd, incrementally=True)
        except Exception as exc:
            dl_note = "%s: %s" % (type(exc).__name__, str(exc)[:200].replace("\n", " "))
            print("[1m-dl-exc] %s %.2fs %s" % (code, _time.time() - t_dl, dl_note))

        try:
            frame = _read_1m()
        except Exception as exc:
            print("[1m-read-exc] %s dl=%.2fs rf=%.2fs %s: %s"
                  % (code, _time.time() - t_dl, _time.time() - t_rf, type(exc).__name__,
                     str(exc)[:200].replace("\n", " ")))
            return ("fail", code)

        if frame is not None and not frame.empty and not _has_vol(frame):
            print("[1m-zero-vol] %s rows=%d volume_all_zero (treated as empty)"
                  % (code, len(frame)))
            frame = None
        if frame is None or frame.empty:
            if rf_exc:
                print("[1m-read-first-exc] %s rf=%.2fs %s" % (code, _time.time() - t_rf, rf_exc))
            print("[1m-empty] %s dl=%.2fs rf=%.2fs dl_exc=%s"
                  % (code, _time.time() - t_dl, _time.time() - t_rf, dl_note or "none"))
            return ("fail", code) if dl_note else ("empty", code)

    day_df = frame.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
    if day_df.empty:
        return ("empty", code)
    # 2026-10-20: 兜底闸门 —— 全零成交绝不写库
    try:
        if int(day_df["volume"].sum()) == 0:
            print("[1m-zero-vol] %s rows=%d volume_all_zero on write, skipped"
                  % (code, len(day_df)))
            return ("empty", code)
    except Exception:
        pass

    # 写当日 parquet（低压缩，快速写入）
    try:
        _os.makedirs(code_dir, exist_ok=True)
        pq.write_table(
            pa.Table.from_pandas(day_df, preserve_index=False),
            day_file,
            compression="zstd",
            compression_level=1,
        )
    except Exception:
        return ("fail", code)

    return ("ok", code)


def main():
    parser = argparse.ArgumentParser(description="增量更新 1m K-line 数据（多进程，日级 parquet）")
    parser.add_argument("--workers", type=int, default=4,
                        help="并行进程数 (default: 4)")
    args = parser.parse_args()

    # 2026-10-16: out root overridable via PIPE_OUT_BASE (默认与原来完全一致, 生产零变化).
    #   为什么加：`--limit` 本就是"测试用"开关，而 out 根目录写死 ⇒ 盘中用生产脚本压测
    #   会把当天（只剩集合竞价）的假文件写进**生产目录**，生产侧的 "skip if exists"
    #   又把它们当成"已完成"永久留下（10-16 事故：648 只 tick 只剩 09:15-09:24 的行，
    #   16:00 归档 Skip=648 一跳而过、计数类判据全绿）。
    #   压测请显式隔离：set PIPE_OUT_BASE=D:\qmt_data_parquet_test\<tick_parquet|kline_1m>
    out_dir_base = os.environ.get("PIPE_OUT_BASE", r"D:\qmt_data_parquet\kline_1m")
    data_dir = "D:\\qmt_data"
    os.makedirs(out_dir_base, exist_ok=True)

    # 股票池
    import xtquant.xtdata as xtdata
    xtdata.data_dir = data_dir
    sh_sz = xtdata.get_stock_list_in_sector("沪深A股") or []
    bj = xtdata.get_stock_list_in_sector("京市A股") or []
    stocks = sorted(set(sh_sz) | set(bj))
    print(f"股票池: {len(stocks)} 只（SH+SZ={len(sh_sz)}, BJ={len(bj)}）")

    today = datetime.now()
    today_ymd = today.strftime("%Y%m%d")
    today_dash = today.strftime("%Y-%m-%d")

    print(f"{len(stocks)} stocks, {args.workers} workers, updating 1m data...")

    worker_args = [
        (code, out_dir_base, data_dir, today_ymd, today_dash)
        for code in stocks
    ]

    import sys as _sys

    t0 = datetime.now()
    ok = fail = skip = empty = 0
    retry_codes = []

    from tqdm import tqdm
    with Pool(processes=args.workers) as pool:
        for status, code in tqdm(pool.imap_unordered(process_stock, worker_args),
                                  total=len(stocks), desc="1m(MP)", unit="stock"):
            if status == "ok":
                ok += 1
            elif status == "fail":
                fail += 1
                retry_codes.append(code)
            elif status == "empty":
                empty += 1
                retry_codes.append(code)
            else:
                skip += 1

    # 2026-09-16: 失败/空读有界补跑一遍（原实现混进 skip => 静默漏数据）
    recovered = still_fail = still_empty = 0
    if retry_codes:
        _pick = set(retry_codes)
        retry_args = [a for a in worker_args if a[0] in _pick]
        print("\n[retry] 补跑 %d 只（fail+empty）" % len(retry_args))
        with Pool(processes=args.workers) as pool:
            for status, code in tqdm(pool.imap_unordered(process_stock, retry_args),
                                     total=len(retry_args), desc="1m(retry)", unit="stock"):
                if status == "ok":
                    recovered += 1
                elif status == "empty":
                    still_empty += 1
                elif status == "fail":
                    still_fail += 1

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\nDone! OK={ok} Skip={skip} Fail={fail} Empty={empty} "
          f"Retry={len(retry_codes)} Recovered={recovered} Elapsed={elapsed:.0f}s")
    StillFailing = still_fail
    StillEmpty = still_empty
    if StillFailing or empty > 100:
        print(f"[ALERT] 1m 补跑后仍失败 {StillFailing} 只 / 空读 {StillEmpty} 只"
              f"（空读阈值 100，mini 基线 44）—— 当日数据可能不完整")
        _sys.exit(1)
    print(f"Output: {out_dir_base}/{{code}}/{{date}}.parquet")


if __name__ == "__main__":
    main()
