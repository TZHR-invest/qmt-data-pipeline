"""
Tick 数据增量更新 + 按日 Parquet 导出（多进程版）

用法：  python update_all_tick_mp.py
       python update_all_tick_mp.py --workers 4

与 update_all_tick.py 区别：使用 multiprocessing.Pool 并行处理，适合日常盘后快速增量。
"""

import sys, os, argparse
from datetime import datetime, timedelta
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
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

    # 第1步：增量下载
    try:
        xtdata.download_history_data(code, "tick", start_date, end, incrementally=True)
    except Exception:
        return ("fail", code)

    # 第2步：只读今天
    try:
        raw = xtdata.get_market_data(
            field_list=[], stock_list=[code], period="tick",
            start_time=today_ymd, end_time=today_ymd, count=-1,
        )
        arr = raw.get(code) if raw else None
    except Exception:
        return ("fail", code)

    if arr is None or len(arr) == 0:
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
    bj = xtdata.get_stock_list_in_sector("BJ") or []
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
