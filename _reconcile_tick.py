"""
每周运行：扫描 .DAT 中 90 天的数据，补齐 parquet 缺失的日期
用法： python _reconcile_tick.py [--workers 4]
"""
import sys, os, argparse
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
# 2026-09-15: mini 退役后 miniquote(58610) 消失，行情改走大QMT 桥。
# 必须插在 _WORKSPACE 之后：G:\qmt_projects\xtquant 是旧 SDK 副本(与 venv 内那份
# 逐字节相同)，会遮蔽桥的影子包；spawn 子进程会重跑本模块体，故对子进程同样生效。
sys.path.insert(0, r"C:\bridge-client")
sys.path.insert(0, r"C:\bridge-client\bridge\src")
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from multiprocessing import Pool

out_dir = r"D:\qmt_data_parquet\tick_parquet"
data_dir = "D:\\qmt_data"

def reconcile_stock(args):
    code, today_ymd, start_3m = args

    from datetime import datetime, timedelta
    import xtquant.xtdata as xtdata
    xtdata.data_dir = data_dir

    code_dir = os.path.join(out_dir, code.replace(".", "_"))
    os.makedirs(code_dir, exist_ok=True)

    # 识别 parquet 已有哪些日期
    existing = set()
    if os.path.isdir(code_dir):
        for f in os.listdir(code_dir):
            if f.endswith(".parquet"):
                existing.add(f.replace(".parquet", ""))

    # 如果已有文件已覆盖整个范围，跳过
    all_dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(90)]
    if existing and all(d in existing for d in all_dates):
        return ("skip", code, "已完整")

    # 读 .DAT 中 90 天数据
    try:
        # 2026-09-15: 桥的终端进程内没有 numpy，旧 get_market_data() 会抛
        # RpcServerRepliedError(ModuleNotFoundError: numpy)；改用 get_market_data_ex。
        # 注意：桥下 tick 只有 18 列（比 mini 少 tickvol/pe；pe 在 mini 时代恒 0）。
        raw = xtdata.get_market_data_ex(
            field_list=[], stock_list=[code], period="tick",
            start_time=start_3m, end_time=today_ymd, count=-1,
        )
        arr = raw.get(code) if raw else None
    except Exception:
        return ("fail", code, "get_market_data error")

    if arr is None or len(arr) == 0:
        return ("skip", code, "no data in range")

    df = pd.DataFrame(arr)
    df["_date"] = pd.to_datetime(df["time"], unit="ms").dt.strftime("%Y-%m-%d")

    written = 0
    for date_str, day_df in df.groupby("_date"):
        if date_str in existing:
            continue  # 已有，跳过
        day_df = day_df.drop(columns=["_date"]).sort_values("time").reset_index(drop=True)
        pq.write_table(
            pa.Table.from_pandas(day_df, preserve_index=False),
            os.path.join(code_dir, f"{date_str}.parquet"),
            compression="zstd", compression_level=6,
        )
        written += 1

    if written:
        return ("ok", code, f"补了 {written} 天")
    return ("skip", code, "已完整")

def main():
    parser = argparse.ArgumentParser(description="对账：补齐 parquet 缺失日期的 tick 数据")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, default=0, help="限制数量，用于测试")
    args = parser.parse_args()

    import xtquant.xtdata as xtdata
    xtdata.data_dir = data_dir

    sh_sz = xtdata.get_stock_list_in_sector("沪深A股") or []
    bj = xtdata.get_stock_list_in_sector("京市A股") or []
    stocks = sorted(set(sh_sz) | set(bj))
    if args.limit:
        stocks = stocks[:args.limit]

    today = datetime.now()
    today_ymd = today.strftime("%Y%m%d")
    start_3m = (today - timedelta(days=90)).strftime("%Y%m%d")

    print(f"对账 {len(stocks)} 只股票，范围 {start_3m} ~ {today_ymd}, workers={args.workers}")

    worker_args = [(code, today_ymd, start_3m) for code in stocks]

    from tqdm import tqdm
    ok = skip = fail = 0
    t0 = datetime.now()

    with Pool(processes=args.workers) as pool:
        for status, code, msg in tqdm(pool.imap_unordered(reconcile_stock, worker_args),
                                       total=len(stocks), desc="Reconcile", unit="stock"):
            if status == "ok":
                ok += 1
            elif status == "fail":
                fail += 1
            else:
                skip += 1

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\nDone! 补齐={ok} 跳过={skip} 失败={fail} 耗时={elapsed:.0f}s")

if __name__ == "__main__":
    main()
