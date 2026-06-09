"""
一次性任务：将 QMT 中的全量 1m 历史数据导出到按月分片 parquet

用法： python backfill_1m_to_monthly.py [--workers 8] [--force]
      --force  覆盖已存在的月文件
"""
import sys, os, argparse
from multiprocessing import Pool
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

out_dir = os.path.join(_WORKSPACE, "kline_1m")
data_dir = "G:\\qmt\\userdata_mini\\datadir"


def export_stock(args):
    """导出单个股票的 1m 全量数据为按月 parquet"""
    code, force = args

    import xtquant.xtdata as xtdata
    xtdata.data_dir = data_dir

    code_key = code.replace(".", "_")
    code_dir = os.path.join(out_dir, code_key)
    os.makedirs(code_dir, exist_ok=True)

    # 检查已完成的月份
    existing_months = set()
    if os.path.isdir(code_dir):
        for f in os.listdir(code_dir):
            if f.endswith(".parquet") and len(f) == 11:  # YYYY-MM.parquet
                if not force:
                    existing_months.add(f.replace(".parquet", ""))

    # 先确保数据已缓存到 QMT（增量下载，只补缺失）
    try:
        xtdata.download_history_data(code, "1m", "20250101", "20260609", incrementally=True)
    except Exception:
        pass  # 下载失败仍尝试读缓存

    # 读取全量 1m 数据
    try:
        raw = xtdata.get_market_data_ex(
            field_list=[], stock_list=[code], period="1m",
            start_time="", end_time="", count=-1,
        )
        frame = raw.get(code) if raw else None
    except Exception:
        return ("fail", code, "get_data failed")

    if frame is None or frame.empty:
        return ("skip", code, "no data")

    # 提取月份，按组分批写入
    # frame.index = string "YYYYMMDDHHMMSS", time 列 = int64 ms
    months = frame.index.str[:6]  # YYYYMM
    written = 0
    skipped = 0
    for ym, grp in frame.groupby(months, sort=True):
        if ym in existing_months:
            skipped += 1
            continue
        day_df = grp.reset_index(drop=True)  # keep 'time' column, drop index
        day_df = day_df.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)

        month_file = os.path.join(code_dir, f"{ym[:4]}-{ym[4:6]}.parquet")
        pq.write_table(
            pa.Table.from_pandas(day_df, preserve_index=False),
            month_file,
            compression="zstd", compression_level=6,
        )
        written += 1

    if written == 0 and skipped == 0:
        return ("skip", code, f"{len(frame)} rows, 0 months (?)")
    return ("ok", code, f"{len(frame)} rows -> {written} monthly files, {skipped} skipped")


def _worker(args):
    return export_stock(args)


def main():
    parser = argparse.ArgumentParser(description="回填 1m 历史数据到按月 parquet")
    parser.add_argument("--workers", type=int, default=0,
                        help="并行进程数 (default: CPU 核心数)")
    parser.add_argument("--force", action="store_true",
                        help="覆盖已存在的月文件")
    parser.add_argument("--limit", type=int, default=0,
                        help="限制处理数量（用于测试）")
    args = parser.parse_args()

    # 获取股票列表
    import xtquant.xtdata as xtdata
    xtdata.data_dir = data_dir
    sh_sz = xtdata.get_stock_list_in_sector("沪深A股") or []
    bj = xtdata.get_stock_list_in_sector("BJ") or []
    stocks = sorted(set(sh_sz) | set(bj))
    print(f"股票池: {len(stocks)} 只")

    if args.limit:
        stocks = stocks[:args.limit]
        print(f"限制处理 {args.limit} 只")

    workers = args.workers or os.cpu_count() or 4
    print(f"使用 {workers} 个 worker")

    t0 = __import__("time").time()
    ok = fail = skip = 0

    from tqdm import tqdm
    worker_args = [(code, args.force) for code in stocks]
    with Pool(processes=workers) as pool:
        for status, code, msg in tqdm(
            pool.imap_unordered(_worker, worker_args),
            total=len(stocks), desc="1m backfill", unit="stock"
        ):
            if status == "ok":
                ok += 1
            elif status == "fail":
                fail += 1
            else:
                skip += 1
            tqdm.write(f"  [{code}] {msg}")

    elapsed = __import__("time").time() - t0
    print(f"\n完成! OK={ok} Skip={skip} Fail={fail} 耗时={elapsed:.0f}s ({elapsed/60:.1f}min)")


if __name__ == "__main__":
    main()
