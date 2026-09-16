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
    t_dl = time.time()
    dl_note = ""
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
        # 2026-09-15: 桥的终端进程内没有 numpy，旧 get_market_data() 会抛
        # RpcServerRepliedError(ModuleNotFoundError: numpy)；改用 get_market_data_ex。
        # 注意：桥下 tick 只有 18 列（比 mini 少 tickvol/pe；pe 在 mini 时代恒 0）。
        raw = xtdata.get_market_data_ex(
            field_list=[], stock_list=[code], period="tick",
            start_time=today_ymd, end_time=today_ymd, count=-1,
        )
        arr = raw.get(code) if raw else None
    except Exception as exc:
        print("[tick-read-exc] %s dl=%.2fs %s: %s"
              % (code, time.time() - t_dl, type(exc).__name__,
                 str(exc)[:200].replace("\n", " ")))
        return ("skip", code)

    if arr is None or len(arr) == 0:
        print("[tick-empty] %s dl=%.2fs dl_exc=%s"
              % (code, time.time() - t_dl, dl_note or "none"))
        return ("skip", code)

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

    t0 = datetime.now()
    ok = fail = skip = 0

    from tqdm import tqdm
    with Pool(processes=args.workers) as pool:
        for status, code in tqdm(pool.imap_unordered(process_stock, worker_args),
                                  total=len(stocks), desc="Tick(MP)", unit="stock"):
            if status == "ok":
                ok += 1
            elif status == "fail":
                fail += 1
            else:
                skip += 1

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\nDone! OK={ok} Skip={skip} Fail={fail} Elapsed={elapsed:.0f}s")
    print(f"Output: {out_dir_base}/{{code}}/{{date}}.parquet")
