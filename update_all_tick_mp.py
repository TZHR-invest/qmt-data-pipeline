"""
Tick 数据增量更新 + 按日 Parquet 导出（多进程版）

用法：  python update_all_tick_mp.py
       python update_all_tick_mp.py --workers 4

与 update_all_tick.py 区别：使用 multiprocessing.Pool 并行处理，适合日常盘后快速增量。
"""

import sys, os, argparse, time
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
    """单个股票的 tick 下载 + parquet 导出（每个进程独立运行）"""
    code, out_dir_base, data_dir, start_date, end, today_ymd, today_dash = args

    import xtquant.xtdata as xtdata
    import pyarrow as pa
    import pyarrow.parquet as pq

    xtdata.data_dir = data_dir

    code_dir = os.path.join(out_dir_base, code.replace(".", "_"))
    day_file = os.path.join(code_dir, f"{today_dash}.parquet")

    # 跳过已完成的
    if os.path.exists(day_file) and os.path.getsize(day_file) > 0:
        return ("skip", code)

    # 增量下载今天 tick（必须显式 download，get_market_data 不会自动拉当天数据）
    # 2026-09-16: 原为 except: pass — 首轮 2828 只 skip 未留下任何原因（日志里
    # timeout/error/Traceback 计数全 0，无法定位）。以下只加诊断，不改控制流。
    # ── 2026-10-14 读优先（read-first）────────────────────────────────────────
    # 依据（实测）：桥下 download_history_data 已是空转（客户端配置 download_jobs_enabled=False；
    # 冷日期调用它 60s 后数据依然 0 行；热日期"先读"与"dl+read"逐值相同）。它白花
    # ~1.4–1.7s/票，且对空票白等 60s（10-14 Empty=5 ⇒ 白花约 5 min）。
    # 故：先读一次；读到就用；读不到才回落到原来的 download+read（分类语义完全不变）。
    def _read_tick():
        _raw = xtdata.get_market_data_ex(
            field_list=[], stock_list=[code], period="tick",
            start_time=today_ymd, end_time=today_ymd, count=-1,
        )
        return _raw.get(code) if _raw else None

    # 开关（默认开；PIPE_READ_FIRST=0 强制走老路径 —— 审计/回退用，不必改代码）
    _rf_on = os.environ.get("PIPE_READ_FIRST", "1") not in ("0", "false", "no", "off")
    t_rf = time.time()
    arr = None
    rf_exc = ""
    if _rf_on:
        try:
            arr = _read_tick()
        except Exception as exc:
            rf_exc = "%s: %s" % (type(exc).__name__, str(exc)[:200].replace("\n", " "))

    dl_note = ""
    if _rf_on and arr is not None and len(arr) > 0:
        print("[tick-read-first] %s %.2fs rows=%d" % (code, time.time() - t_rf, len(arr)))
    else:
        # 回落：老路径（download 异常 ⇒ dl_note 非空 ⇒ 空读判 fail 而非 empty）
        t_dl = time.time()
        try:
            xtdata.download_history_data(code, "tick", start_date, end, incrementally=True)
        except Exception as exc:
            dl_note = "%s: %s" % (type(exc).__name__, str(exc)[:200].replace("\n", " "))
            print("[tick-dl-exc] %s %.2fs %s" % (code, time.time() - t_dl, dl_note))
        else:
            dl_sec = time.time() - t_dl
            if dl_sec > 3.0:
                print("[tick-dl-slow] %s %.2fs" % (code, dl_sec))

        try:
            arr = _read_tick()
        except Exception as exc:
            print("[tick-read-exc] %s dl=%.2fs rf=%.2fs %s: %s"
                  % (code, time.time() - t_dl, time.time() - t_rf, type(exc).__name__,
                     str(exc)[:200].replace("\n", " ")))
            return ("fail", code)

        if arr is None or len(arr) == 0:
            if rf_exc:
                print("[tick-read-first-exc] %s rf=%.2fs %s" % (code, time.time() - t_rf, rf_exc))
            print("[tick-empty] %s dl=%.2fs rf=%.2fs dl_exc=%s"
                  % (code, time.time() - t_dl, time.time() - t_rf, dl_note or "none"))
            # 下载本身就异常 -> 这是故障；下载正常却无数据 -> 合法空（退市/停牌）
            return ("fail", code) if dl_note else ("empty", code)

    df_new = pd.DataFrame(arr).sort_values("time").reset_index(drop=True)

    # 第3步：写按日 parquet
    os.makedirs(code_dir, exist_ok=True)
    if os.path.exists(day_file):
        df_old = pd.read_parquet(day_file)
        df = pd.concat([df_old, df_new], ignore_index=True)
        df = df.drop_duplicates(subset=["time"], keep="last").sort_values("time").reset_index(drop=True)
    else:
        df = df_new

    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        day_file,
        compression="zstd",
        compression_level=6,
    )
    return ("ok", code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="增量更新 Tick 数据（多进程）")
    parser.add_argument("--workers", type=int, default=4, help="并行进程数 (default: 4)")
    args = parser.parse_args()

    out_dir_base = r"D:\qmt_data_parquet\tick_parquet"
    data_dir = "D:\\qmt_data"
    os.makedirs(out_dir_base, exist_ok=True)

    # 股票池：全部从 xtdata 实时拉取（SH+SZ+BJ，含新股）
    import xtquant.xtdata as xtdata
    xtdata.data_dir = data_dir
    sh_sz = xtdata.get_stock_list_in_sector("沪深A股") or []
    bj = xtdata.get_stock_list_in_sector("京市A股") or []
    stocks = sorted(set(sh_sz) | set(bj))
    print(f"股票池: {len(stocks)} 只（SH+SZ={len(sh_sz)}, BJ={len(bj)}）")

    today = datetime.now()
    today_ymd = today.strftime("%Y%m%d")
    # 增量更新只扫当天，已有历史数据缓存在 QMT 中
    start_date = today_ymd
    end = today_ymd
    today_dash = today.strftime("%Y-%m-%d")

    print(f"{len(stocks)} stocks, {args.workers} workers, updating tick data...")

    worker_args = [
        (code, out_dir_base, data_dir, start_date, end, today_ymd, today_dash)
        for code in stocks
    ]

    import sys as _sys

    t0 = datetime.now()
    ok = fail = skip = empty = 0
    retry_codes = []

    from tqdm import tqdm
    with Pool(processes=args.workers) as pool:
        for status, code in tqdm(pool.imap_unordered(process_stock, worker_args),
                                  total=len(stocks), desc="Tick(MP)", unit="stock"):
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
                                     total=len(retry_args), desc="Tick(retry)", unit="stock"):
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
        print(f"[ALERT] tick 补跑后仍失败 {StillFailing} 只 / 空读 {StillEmpty} 只"
              f"（空读阈值 100，mini 基线 32）—— 当日数据可能不完整")
        _sys.exit(1)
    print(f"Output: {out_dir_base}/{{code}}/{{date}}.parquet")
