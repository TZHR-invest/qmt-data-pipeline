import sys, os, time
from datetime import datetime
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xtquant.xtdata as xtdata
from tqdm import tqdm

xtdata.data_dir = "G:\\qmt\\userdata_mini\\datadir"
out_dir_base = os.path.join(_WORKSPACE, "kline_1m")
os.makedirs(out_dir_base, exist_ok=True)

# 股票池：全部从 xtdata 实时拉取（SH+SZ+BJ，含新股）
sh_sz = xtdata.get_stock_list_in_sector("沪深A股") or []
bj = xtdata.get_stock_list_in_sector("BJ") or []
stocks = sorted(set(sh_sz) | set(bj))
print(f"股票池: {len(stocks)} 只（SH+SZ={len(sh_sz)}, BJ={len(bj)}）")
print(f"{len(stocks)} stocks, incremental update 1m data + parquet export", flush=True)

today = datetime.now()
today_ymd = today.strftime("%Y%m%d")
today_file = today.strftime("%Y-%m-%d")

ok = fail = 0
t0 = time.time()

for code in tqdm(stocks, desc="1m", unit="stock"):
    code_dir = os.path.join(out_dir_base, code.replace(".", "_"))
    day_file = os.path.join(code_dir, f"{today_file}.parquet")

    # 如果今日 parquet 已存在，跳过（代表已导出过）
    if os.path.exists(day_file) and os.path.getsize(day_file) > 0:
        ok += 1
        continue

    # 第1步：增量下载 1m
    try:
        r = xtdata.download_history_data(code, "1m")
        if r != 0:
            fail += 1
            continue
    except Exception:
        fail += 1
        continue

    # 第2步：读本地数据（只读当天）
    try:
        raw = xtdata.get_market_data_ex(
            field_list=[], stock_list=[code], period="1m",
            start_time=today_ymd, end_time=today_ymd, count=-1,
        )
    except Exception:
        fail += 1
        continue

    frame = raw.get(code) if raw else None
    if frame is None or frame.empty:
        ok += 1
        continue

    # 第3步：写按日 parquet
    os.makedirs(code_dir, exist_ok=True)
    day_df = frame.reset_index()
    day_df.columns = ["time"] + list(day_df.columns[1:])  # 确保时间列名为 time
    day_df = day_df.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)

    if os.path.exists(day_file):
        df_old = pd.read_parquet(day_file)
        day_df = pd.concat([df_old, day_df], ignore_index=True)
        day_df = day_df.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)

    pq.write_table(
        pa.Table.from_pandas(day_df, preserve_index=False),
        day_file,
        compression="zstd",
        compression_level=6,
    )
    ok += 1

t = time.time() - t0
print(f"\nDone! OK={ok} Fail={fail} Elapsed={t:.0f}s", flush=True)
print(f"Output: {out_dir_base}/{{code}}/{{date}}.parquet", flush=True)
