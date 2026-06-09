"""
1m K-line 增量更新 + 按月 Parquet 导出（多进程版）

用法： python update_all_1m_mp.py
      python update_all_1m_mp.py --workers 8

与 update_all_1m.py 区别：使用 multiprocessing.Pool 并行处理。
按月分片：每只股票每月一个 parquet 文件（YYYY-MM.parquet）。
"""
import sys, os, argparse
from datetime import datetime, timedelta
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
import pandas as pd
from multiprocessing import Pool


def process_stock(args):
    """单个股票的 1m 下载 + 追加到当月 parquet"""
    code, out_dir_base, data_dir, today_ymd, month_str = args

    import os as _os
    _os.environ["OMP_NUM_THREADS"] = "1"  # 防止 pyarrow 线程泄漏
    import xtquant.xtdata as xtdata
    import pyarrow as pa
    import pyarrow.parquet as pq

    xtdata.data_dir = data_dir

    code_dir = os.path.join(out_dir_base, code.replace(".", "_"))
    month_file = os.path.join(code_dir, f"{month_str}.parquet")

    # 增量下载（确保数据完整）
    try:
        xtdata.download_history_data(code, "1m", today_ymd, today_ymd, incrementally=True)
    except Exception:
        pass

    # 读当天数据
    try:
        raw = xtdata.get_market_data_ex(
            field_list=[], stock_list=[code], period="1m",
            start_time=today_ymd, end_time=today_ymd, count=-1,
        )
        frame = raw.get(code) if raw else None
    except Exception:
        frame = None

    if frame is None or frame.empty:
        return ("skip", code)

    # 第3步：追加到当月 parquet
    os.makedirs(code_dir, exist_ok=True)
    day_df = frame.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)

    if os.path.exists(month_file) and os.path.getsize(month_file) > 0:
        df_old = pd.read_parquet(month_file)
        merged = pd.concat([df_old, day_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
    else:
        merged = day_df

    pq.write_table(
        pa.Table.from_pandas(merged, preserve_index=False),
        month_file,
        compression="zstd",
        compression_level=6,
    )
    return ("ok", code)


def main():
    parser = argparse.ArgumentParser(description="增量更新 1m K-line 数据（多进程）")
    parser.add_argument("--workers", type=int, default=4,
                        help="并行进程数 (default: 4)")
    args = parser.parse_args()

    out_dir_base = os.path.join(_WORKSPACE, "kline_1m")
    data_dir = "G:\\qmt\\userdata_mini\\datadir"
    os.makedirs(out_dir_base, exist_ok=True)

    # 股票池
    import xtquant.xtdata as xtdata
    xtdata.data_dir = data_dir
    sh_sz = xtdata.get_stock_list_in_sector("沪深A股") or []
    bj = xtdata.get_stock_list_in_sector("BJ") or []
    stocks = sorted(set(sh_sz) | set(bj))
    print(f"股票池: {len(stocks)} 只（SH+SZ={len(sh_sz)}, BJ={len(bj)}）")

    today = datetime.now()
    today_ymd = today.strftime("%Y%m%d")
    month_str = today.strftime("%Y-%m")

    print(f"{len(stocks)} stocks, {args.workers} workers, updating 1m data...")

    worker_args = [
        (code, out_dir_base, data_dir, today_ymd, month_str)
        for code in stocks
    ]

    t0 = datetime.now()
    ok = fail = skip = 0

    from tqdm import tqdm
    with Pool(processes=args.workers) as pool:
        for status, code in tqdm(pool.imap_unordered(process_stock, worker_args),
                                  total=len(stocks), desc="1m(MP)", unit="stock"):
            if status == "ok":
                ok += 1
            elif status == "fail":
                fail += 1
            else:
                skip += 1

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\nDone! OK={ok} Skip={skip} Fail={fail} Elapsed={elapsed:.0f}s")
    print(f"Output: {out_dir_base}/{{code}}/{{month}}.parquet")


if __name__ == "__main__":
    main()
