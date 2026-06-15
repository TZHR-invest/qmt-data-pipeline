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

    # 读当天数据（get_market_data_ex 内部会缓存/服务器自动兜底）
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

    day_df = frame.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
    if day_df.empty:
        return ("skip", code)

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

    out_dir_base = r"D:\qmt_data_parquet\kline_1m"
    data_dir = "D:\\qmt_data"
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
    today_dash = today.strftime("%Y-%m-%d")

    print(f"{len(stocks)} stocks, {args.workers} workers, updating 1m data...")

    worker_args = [
        (code, out_dir_base, data_dir, today_ymd, today_dash)
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
    print(f"Output: {out_dir_base}/{{code}}/{{date}}.parquet")


if __name__ == "__main__":
    main()
